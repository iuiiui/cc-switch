//! 响应处理器模块
//!
//! 统一处理流式和非流式 API 响应

use super::{
    content_encoding::{decompress_body_with_limit, get_content_encoding, DecompressError},
    forwarder::ActiveConnectionGuard,
    handler_config::{StreamUsageEventFilter, UsageParserConfig},
    handler_context::{RequestContext, StreamingTimeoutConfig},
    hyper_client::{ProxyResponse, MAX_RESPONSE_BODY_BYTES},
    server::ProxyState,
    sse::{strip_sse_field, take_sse_block},
    usage::parser::TokenUsage,
    ProxyError,
};
use crate::database::PRICING_SOURCE_REQUEST;
use axum::http::{header::HeaderMap, HeaderName};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use futures::stream::{Stream, StreamExt};
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::Mutex;

// ============================================================================
// 响应头处理
// ============================================================================

/// RFC 2616 / RFC 7230 中定义的不应被代理继续转发的响应头。
const HOP_BY_HOP_RESPONSE_HEADERS: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "proxy-connection",
    "te",
    "trailer",
    "trailers",
    "transfer-encoding",
    "upgrade",
];

/// 移除响应侧 hop-by-hop 头，以及 `Connection` 中点名的扩展头。
pub(crate) fn strip_hop_by_hop_response_headers(headers: &mut HeaderMap) {
    let connection_listed_headers: Vec<HeaderName> = headers
        .get_all(axum::http::header::CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .filter_map(|name| HeaderName::from_bytes(name.as_bytes()).ok())
        .collect();

    for name in HOP_BY_HOP_RESPONSE_HEADERS {
        headers.remove(*name);
    }

    for name in connection_listed_headers {
        headers.remove(name);
    }
}

/// 移除在重建响应体后会失真的实体头。
pub(crate) fn strip_entity_headers_for_rebuilt_body(headers: &mut HeaderMap) {
    headers.remove(axum::http::header::CONTENT_ENCODING);
    headers.remove(axum::http::header::CONTENT_LENGTH);
    headers.remove(axum::http::header::TRANSFER_ENCODING);
}

/// 读取响应体并在需要时解压，确保 headers 与返回 body 一致。
///
/// `body_timeout`: 整包超时。当非零时用 `tokio::time::timeout` 包住 `.bytes()` 调用，
/// 防止上游发完响应头后卡住 body 导致请求永远挂住。
/// 传入 `Duration::ZERO` 表示不启用超时（故障转移关闭时）。
pub(crate) async fn read_decoded_body(
    response: ProxyResponse,
    tag: &str,
    body_timeout: Duration,
) -> Result<(HeaderMap, http::StatusCode, Bytes), ProxyError> {
    let mut headers = response.headers().clone();
    let status = response.status();
    let bytes_future = response.bytes_with_limit(MAX_RESPONSE_BODY_BYTES);
    let raw_bytes = if body_timeout.is_zero() {
        bytes_future.await?
    } else {
        tokio::time::timeout(body_timeout, bytes_future)
            .await
            .map_err(|_| {
                ProxyError::Timeout(format!(
                    "响应体读取超时: {}s（上游发完响应头后 body 未到达）",
                    body_timeout.as_secs()
                ))
            })??
    };

    log::debug!(
        "[{tag}] 已接收上游响应体: status={}, bytes={}, headers={}",
        status.as_u16(),
        raw_bytes.len(),
        format_headers(&headers)
    );

    let mut body_bytes = raw_bytes.clone();
    let mut decoded = false;

    if let Some(encoding) = get_content_encoding(&headers) {
        log::debug!("[{tag}] 解压非流式响应: content-encoding={encoding}");
        match decompress_body_with_limit(&encoding, &raw_bytes, MAX_RESPONSE_BODY_BYTES) {
            Ok(Some(decompressed)) => {
                // 解码器在预算耗尽处即截停，此处必然 ≤ MAX_RESPONSE_BODY_BYTES
                body_bytes = Bytes::from(decompressed);
                decoded = true;
            }
            // 不支持的编码：原样透传且保留 content-encoding 头，
            // 让下游诊断/客户端知道这仍是压缩字节
            Ok(None) => {}
            Err(DecompressError::TooLarge { .. }) => {
                return Err(ProxyError::ResponseBodyTooLarge(MAX_RESPONSE_BODY_BYTES));
            }
            Err(DecompressError::Io(e)) => {
                log::warn!("[{tag}] 解压失败 ({encoding}): {e}，使用原始数据");
            }
        }
    }

    if decoded {
        strip_entity_headers_for_rebuilt_body(&mut headers);
    }

    Ok((headers, status, body_bytes))
}

// ============================================================================
// 公共接口
// ============================================================================

/// 检测响应是否为 SSE 流式响应
#[inline]
pub fn is_sse_response(response: &ProxyResponse) -> bool {
    response.is_sse()
}

/// 处理流式响应
pub async fn handle_streaming(
    response: ProxyResponse,
    ctx: &RequestContext,
    state: &ProxyState,
    parser_config: &UsageParserConfig,
    connection_guard: Option<ActiveConnectionGuard>,
) -> Response {
    let status = response.status();
    log::debug!(
        "[{}] 已接收上游流式响应: status={}, headers={}",
        ctx.tag,
        status.as_u16(),
        format_headers(response.headers())
    );
    // 检查流式响应是否被压缩（SSE 通常不压缩，如果压缩则 SSE 解析会失败）
    if let Some(encoding) = get_content_encoding(response.headers()) {
        log::warn!(
            "[{}] 流式响应含 content-encoding={encoding}，SSE 解析可能失败。\
             上游在 accept-encoding 透传后压缩了 SSE 流。",
            ctx.tag
        );
    }

    let mut response_headers = response.headers().clone();
    strip_hop_by_hop_response_headers(&mut response_headers);

    let mut builder = axum::response::Response::builder().status(status);

    // 复制响应头
    for (key, value) in &response_headers {
        builder = builder.header(key, value);
    }

    // 创建字节流
    let stream = response.bytes_stream();

    // 创建使用量收集器；关闭 usage logging 时不要在流式热路径上解析每个 SSE event。
    let usage_collector = create_usage_collector(ctx, state, status.as_u16(), parser_config);

    // 获取流式超时配置
    let timeout_config = ctx.streaming_timeout_config();

    // 创建带日志和超时的透传流
    let logged_stream = create_logged_passthrough_stream(
        stream,
        ctx.tag,
        usage_collector,
        timeout_config,
        connection_guard,
        if ctx.app_type_str == "claude-desktop" {
            SseTerminalPolicy::ClaudeDesktopAnthropic
        } else if ctx.app_type_str == "claude" {
            SseTerminalPolicy::AnthropicMessages
        } else {
            SseTerminalPolicy::Passthrough
        },
    );

    let body = axum::body::Body::from_stream(logged_stream);
    match builder.body(body) {
        Ok(resp) => resp,
        Err(e) => {
            log::error!("[{}] 构建流式响应失败: {e}", ctx.tag);
            ProxyError::Internal(format!("Failed to build streaming response: {e}")).into_response()
        }
    }
}

/// 处理非流式响应
pub async fn handle_non_streaming(
    response: ProxyResponse,
    ctx: &RequestContext,
    state: &ProxyState,
    parser_config: &UsageParserConfig,
    // guard 在函数 scope 内持有，整包响应读取完成后随函数返回一并 drop
    _connection_guard: Option<ActiveConnectionGuard>,
) -> Result<Response, ProxyError> {
    // 整包超时：仅在故障转移开启且配置值非零时生效
    let body_timeout =
        if ctx.app_config.auto_failover_enabled && ctx.app_config.non_streaming_timeout > 0 {
            Duration::from_secs(ctx.app_config.non_streaming_timeout as u64)
        } else {
            Duration::ZERO
        };
    let (mut response_headers, status, body_bytes) =
        read_decoded_body(response, ctx.tag, body_timeout).await?;
    strip_hop_by_hop_response_headers(&mut response_headers);

    log::debug!(
        "[{}] 上游响应体已接收: bytes={} (content omitted)",
        ctx.tag,
        body_bytes.len()
    );

    // 解析并记录使用量。关闭 usage logging 时直接跳过，避免非流式响应整包 JSON parse。
    if usage_logging_enabled(state) {
        if let Ok(json_value) = serde_json::from_slice::<Value>(&body_bytes) {
            // 解析使用量
            if let Some(usage) = (parser_config.response_parser)(&json_value) {
                // 归因优先级：usage 解析出的模型 → 响应 model 字段 → 映射后的出站
                // 模型（路由接管真值）→ 客户端请求模型。空字符串视为缺失。
                let model = usage
                    .model
                    .clone()
                    .filter(|m| !m.is_empty())
                    .or_else(|| {
                        json_value
                            .get("model")
                            .and_then(|m| m.as_str())
                            .filter(|m| !m.is_empty())
                            .map(str::to_string)
                    })
                    .or_else(|| ctx.outbound_model.clone())
                    .unwrap_or_else(|| ctx.request_model.clone());

                spawn_log_usage(
                    state,
                    ctx,
                    usage,
                    &model,
                    &ctx.request_model,
                    status.as_u16(),
                    false,
                );
            } else {
                let model = json_value
                    .get("model")
                    .and_then(|m| m.as_str())
                    .filter(|m| !m.is_empty())
                    .map(str::to_string)
                    .or_else(|| ctx.outbound_model.clone())
                    .unwrap_or_else(|| ctx.request_model.clone());
                spawn_log_usage(
                    state,
                    ctx,
                    TokenUsage::default(),
                    &model,
                    &ctx.request_model,
                    status.as_u16(),
                    false,
                );
                log::debug!(
                    "[{}] 未能解析 usage 信息，跳过记录",
                    parser_config.app_type_str
                );
            }
        } else {
            log::debug!(
                "[{}] <<< 响应 (非 JSON): {} bytes",
                ctx.tag,
                body_bytes.len()
            );
            spawn_log_usage(
                state,
                ctx,
                TokenUsage::default(),
                ctx.outbound_model.as_deref().unwrap_or(&ctx.request_model),
                &ctx.request_model,
                status.as_u16(),
                false,
            );
        }
    } else {
        log::debug!("[{}] usage logging 已关闭，跳过非流式 usage 解析", ctx.tag);
    }

    // 构建响应
    let mut builder = axum::response::Response::builder().status(status);
    for (key, value) in response_headers.iter() {
        builder = builder.header(key, value);
    }

    let body = axum::body::Body::from(body_bytes);
    builder.body(body).map_err(|e| {
        log::error!("[{}] 构建响应失败: {e}", ctx.tag);
        ProxyError::Internal(format!("Failed to build response: {e}"))
    })
}

/// 通用响应处理入口
///
/// 根据响应类型自动选择流式或非流式处理
pub async fn process_response(
    response: ProxyResponse,
    ctx: &RequestContext,
    state: &ProxyState,
    parser_config: &UsageParserConfig,
    connection_guard: Option<ActiveConnectionGuard>,
) -> Result<Response, ProxyError> {
    if is_sse_response(&response) {
        Ok(handle_streaming(response, ctx, state, parser_config, connection_guard).await)
    } else {
        handle_non_streaming(response, ctx, state, parser_config, connection_guard).await
    }
}

// ============================================================================
// SSE 使用量收集器
// ============================================================================

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StreamTerminalOutcome {
    pub status_code_override: Option<u16>,
    pub error_message: Option<String>,
}

type UsageCallbackWithTiming =
    Arc<dyn Fn(Vec<Value>, Option<u64>, StreamTerminalOutcome) + Send + Sync + 'static>;

/// SSE 使用量收集器
#[derive(Clone)]
pub struct SseUsageCollector {
    inner: Arc<SseUsageCollectorInner>,
}

struct SseUsageCollectorInner {
    events: Mutex<Vec<Value>>,
    first_event_time: Mutex<Option<std::time::Instant>>,
    first_event_set: AtomicBool,
    terminal_outcome: Mutex<StreamTerminalOutcome>,
    start_time: std::time::Instant,
    on_complete: UsageCallbackWithTiming,
    should_collect: Option<StreamUsageEventFilter>,
    finished: AtomicBool,
}

impl SseUsageCollector {
    /// 创建使用量收集器；`should_collect` 用来在 hot path 跳过与 usage 无关的事件。
    pub fn new(
        start_time: std::time::Instant,
        should_collect: Option<StreamUsageEventFilter>,
        callback: impl Fn(Vec<Value>, Option<u64>, StreamTerminalOutcome) + Send + Sync + 'static,
    ) -> Self {
        let on_complete: UsageCallbackWithTiming = Arc::new(callback);
        Self {
            inner: Arc::new(SseUsageCollectorInner {
                events: Mutex::new(Vec::new()),
                first_event_time: Mutex::new(None),
                first_event_set: AtomicBool::new(false),
                terminal_outcome: Mutex::new(StreamTerminalOutcome::default()),
                start_time,
                on_complete,
                should_collect,
                finished: AtomicBool::new(false),
            }),
        }
    }

    pub fn should_collect(&self, data: &str) -> bool {
        self.inner
            .should_collect
            .map(|filter| filter(data))
            .unwrap_or(true)
    }

    /// 标记首个被收集的 SSE 事件时间，沿用 `first_token_ms` 的既有近似语义。
    async fn mark_first_collected_event_time(&self) {
        if self.inner.first_event_set.load(Ordering::Acquire) {
            return;
        }
        let mut first_time = self.inner.first_event_time.lock().await;
        if first_time.is_none() {
            *first_time = Some(std::time::Instant::now());
            self.inner.first_event_set.store(true, Ordering::Release);
        }
    }

    /// 推送 SSE 事件
    pub async fn push(&self, event: Value) {
        self.mark_first_collected_event_time().await;
        let mut events = self.inner.events.lock().await;
        events.push(event);
    }

    pub async fn set_terminal_failure(&self, status_code: u16, message: impl Into<String>) {
        let mut outcome = self.inner.terminal_outcome.lock().await;
        if outcome.error_message.is_none() {
            outcome.status_code_override = Some(status_code);
            outcome.error_message = Some(message.into());
        }
    }

    /// 完成收集并触发回调
    pub async fn finish(&self) {
        if self.inner.finished.swap(true, Ordering::SeqCst) {
            return;
        }

        let events = {
            let mut guard = self.inner.events.lock().await;
            std::mem::take(&mut *guard)
        };

        let first_token_ms = {
            let first_time = self.inner.first_event_time.lock().await;
            first_time.map(|t| (t - self.inner.start_time).as_millis() as u64)
        };

        let terminal_outcome = self.inner.terminal_outcome.lock().await.clone();

        (self.inner.on_complete)(events, first_token_ms, terminal_outcome);
    }
}

struct SseUsageFinishGuard {
    collector: Option<SseUsageCollector>,
}

impl SseUsageFinishGuard {
    fn new(collector: SseUsageCollector) -> Self {
        Self {
            collector: Some(collector),
        }
    }

    fn disarm(&mut self) {
        self.collector = None;
    }
}

impl Drop for SseUsageFinishGuard {
    fn drop(&mut self) {
        if let Some(collector) = self.collector.take() {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    collector.finish().await;
                });
            } else {
                log::warn!("SSE 用量收尾保护触发时 Tokio runtime 不可用，跳过异步 finish");
            }
        }
    }
}

// ============================================================================
// 内部辅助函数
// ============================================================================

/// 创建使用量收集器
pub(crate) fn create_usage_collector(
    ctx: &RequestContext,
    state: &ProxyState,
    status_code: u16,
    parser_config: &UsageParserConfig,
) -> Option<SseUsageCollector> {
    let logging_enabled = state
        .config
        .try_read()
        .map(|c| c.enable_logging)
        .unwrap_or(true);
    if !logging_enabled {
        return None;
    }

    let state = state.clone();
    let provider_id = ctx.provider.id.clone();
    let request_model = ctx.request_model.clone();
    // 流式事件缺失模型名时的归因兜底：映射后的出站模型（路由接管真值）优先，
    // 其次才是客户端请求别名
    let fallback_model = ctx
        .outbound_model
        .clone()
        .unwrap_or_else(|| ctx.request_model.clone());
    // 用 ctx 的 app_type 而不是 parser_config 的：Claude Desktop 流式透传复用
    // CLAUDE_PARSER_CONFIG（app_type_str="claude"），按 parser_config 记账会把
    // claude-desktop 的行错记到 claude 名下，导致供应商计价覆盖解析不到。
    let app_type_str = ctx.app_type_str;
    let tag = ctx.tag;
    let start_time = ctx.start_time;
    let stream_parser = parser_config.stream_parser;
    let model_extractor = parser_config.model_extractor;
    let session_id = ctx.session_id.clone();

    Some(SseUsageCollector::new(
        start_time,
        parser_config.stream_event_filter,
        move |events, first_token_ms, terminal_outcome| {
            let final_status = terminal_outcome.status_code_override.unwrap_or(status_code);
            let final_error = terminal_outcome.error_message;
            if let Some(usage) = stream_parser(&events) {
                let model = model_extractor(&events, &fallback_model);
                let latency_ms = start_time.elapsed().as_millis() as u64;

                let state = state.clone();
                let provider_id = provider_id.clone();
                let session_id = session_id.clone();
                let request_model = request_model.clone();
                let outbound_model = fallback_model.clone();

                tokio::spawn(async move {
                    log_usage_internal(
                        &state,
                        &provider_id,
                        app_type_str,
                        &model,
                        &request_model,
                        &outbound_model,
                        usage,
                        latency_ms,
                        first_token_ms,
                        true, // is_streaming
                        final_status,
                        final_error,
                        Some(session_id),
                    )
                    .await;
                });
            } else {
                let model = model_extractor(&events, &fallback_model);
                let latency_ms = start_time.elapsed().as_millis() as u64;
                let state = state.clone();
                let provider_id = provider_id.clone();
                let session_id = session_id.clone();
                let request_model = request_model.clone();
                let outbound_model = fallback_model.clone();

                tokio::spawn(async move {
                    log_usage_internal(
                        &state,
                        &provider_id,
                        app_type_str,
                        &model,
                        &request_model,
                        &outbound_model,
                        TokenUsage::default(),
                        latency_ms,
                        first_token_ms,
                        true, // is_streaming
                        final_status,
                        final_error,
                        Some(session_id),
                    )
                    .await;
                });
                log::debug!("[{tag}] 流式响应缺少 usage 统计，跳过消费记录");
            }
        },
    ))
}

/// 异步记录使用量
fn spawn_log_usage(
    state: &ProxyState,
    ctx: &RequestContext,
    usage: TokenUsage,
    model: &str,
    request_model: &str,
    status_code: u16,
    is_streaming: bool,
) {
    // Check enable_logging before spawning the log task
    if let Ok(config) = state.config.try_read() {
        if !config.enable_logging {
            return;
        }
    }

    let state = state.clone();
    let provider_id = ctx.provider.id.clone();
    let app_type_str = ctx.app_type_str.to_string();
    let model = model.to_string();
    let request_model = request_model.to_string();
    // 「按请求计价」模式的锚点：映射后的出站模型，无映射时等于 request_model
    let outbound_model = ctx
        .outbound_model
        .clone()
        .unwrap_or_else(|| ctx.request_model.clone());
    let latency_ms = ctx.latency_ms();
    let session_id = ctx.session_id.clone();

    tokio::spawn(async move {
        log_usage_internal(
            &state,
            &provider_id,
            &app_type_str,
            &model,
            &request_model,
            &outbound_model,
            usage,
            latency_ms,
            None,
            is_streaming,
            status_code,
            None,
            Some(session_id),
        )
        .await;
    });
}

pub(crate) fn usage_logging_enabled(state: &ProxyState) -> bool {
    state
        .config
        .try_read()
        .map(|config| config.enable_logging)
        .unwrap_or(true)
}

/// 内部使用量记录函数
///
/// `outbound_model` 是「按请求计价」模式的锚点：实际发往上游的模型
/// （路由接管映射后的真值，无映射时等于 request_model）。该模式的语义是
/// 「按代理发出的请求计价、不信任上游回显」，接管场景下发出的请求模型是
/// 映射后的 Y 而非客户端别名 X，按 X 计价会用错定价表行。
#[allow(clippy::too_many_arguments)]
async fn log_usage_internal(
    state: &ProxyState,
    provider_id: &str,
    app_type: &str,
    model: &str,
    request_model: &str,
    outbound_model: &str,
    usage: TokenUsage,
    latency_ms: u64,
    first_token_ms: Option<u64>,
    is_streaming: bool,
    status_code: u16,
    error_message: Option<String>,
    session_id: Option<String>,
) {
    use super::usage::logger::UsageLogger;

    let logger = UsageLogger::new(&state.db);
    let (multiplier, pricing_model_source) =
        logger.resolve_pricing_config(provider_id, app_type).await;
    let pricing_model = if pricing_model_source == PRICING_SOURCE_REQUEST {
        outbound_model
    } else {
        model
    };

    let dedup_scope = super::usage::parser::dedup_scope_for_app(app_type, provider_id);
    let request_id = usage.dedup_request_id(dedup_scope);

    log::debug!(
        "[{app_type}] 记录请求日志: id={request_id}, provider={provider_id}, model={model}, streaming={is_streaming}, status={status_code}, latency_ms={latency_ms}, first_token_ms={first_token_ms:?}, session={}, input={}, output={}, cache_read={}, cache_creation={}",
        session_id.as_deref().unwrap_or("none"),
        usage.input_tokens,
        usage.output_tokens,
        usage.cache_read_tokens,
        usage.cache_creation_tokens
    );

    if let Err(e) = logger.log_with_calculation_outcome(
        request_id,
        provider_id.to_string(),
        app_type.to_string(),
        model.to_string(),
        request_model.to_string(),
        pricing_model.to_string(),
        usage,
        multiplier,
        latency_ms,
        first_token_ms,
        status_code,
        error_message,
        session_id,
        None, // provider_type
        is_streaming,
    ) {
        log::warn!("[USG-001] 记录使用量失败: {e}");
    }
}

/// 创建带日志记录和超时控制的透传流
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SseTerminalPolicy {
    Passthrough,
    AnthropicMessages,
    ClaudeDesktopAnthropic,
}

impl SseTerminalPolicy {
    fn is_anthropic(self) -> bool {
        matches!(self, Self::AnthropicMessages | Self::ClaudeDesktopAnthropic)
    }

    fn is_claude_desktop(self) -> bool {
        self == Self::ClaudeDesktopAnthropic
    }
}

#[derive(Default)]
struct AnthropicTerminalState {
    saw_message_start: bool,
    saw_final_message_delta: bool,
    saw_message_stop: bool,
    saw_error: bool,
    error_status: Option<u16>,
    error_type: Option<String>,
    error_message: Option<String>,
    open_content_blocks: HashMap<u64, String>,
    tool_input_json: HashMap<u64, String>,
    saw_text_block: bool,
    saw_text_delta: bool,
    saw_tool_use_block: bool,
    open_unsafe_content_blocks: HashSet<u64>,
    saw_invalid_non_text_block: bool,
    silent_recovery_requested: bool,
}

struct HeldAnthropicContentBlock {
    block_type: String,
    events: Vec<String>,
    bytes: usize,
}

const MAX_HELD_ANTHROPIC_BLOCK_BYTES: usize = 512 * 1024;
const MAX_UNFRAMED_ANTHROPIC_SSE_BYTES: usize = 512 * 1024;

#[derive(Default)]
struct AnthropicEventDescriptor {
    event_type: String,
    index: Option<u64>,
    block_type: Option<String>,
}

fn describe_anthropic_sse_block(block: &str) -> AnthropicEventDescriptor {
    let named_event = block
        .lines()
        .find_map(|line| strip_sse_field(line, "event"))
        .map(str::trim);
    let data = block
        .lines()
        .filter_map(|line| strip_sse_field(line, "data"))
        .collect::<Vec<_>>()
        .join("\n");
    let parsed = serde_json::from_str::<Value>(&data).ok();
    AnthropicEventDescriptor {
        event_type: parsed
            .as_ref()
            .and_then(|value| value.get("type"))
            .and_then(Value::as_str)
            .or(named_event)
            .unwrap_or("")
            .to_string(),
        index: parsed
            .as_ref()
            .and_then(|value| value.get("index"))
            .and_then(Value::as_u64),
        block_type: parsed
            .as_ref()
            .and_then(|value| value.pointer("/content_block/type"))
            .and_then(Value::as_str)
            .map(ToString::to_string),
    }
}

fn discard_incomplete_anthropic_blocks(
    held: &mut HashMap<u64, HeldAnthropicContentBlock>,
    state: &mut AnthropicTerminalState,
) -> bool {
    let had_incomplete_blocks = !held.is_empty();
    held.clear();
    if had_incomplete_blocks {
        state.silent_recovery_requested = true;
    }
    had_incomplete_blocks
}

fn ensure_anthropic_silent_recovery(state: &mut AnthropicTerminalState) -> Vec<Bytes> {
    let mut events = Vec::new();
    if !state.saw_message_start {
        events.push(
            "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_cc_switch_recovery\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"\",\"content\":[],\"stop_reason\":null,\"stop_sequence\":null,\"usage\":{\"input_tokens\":0,\"output_tokens\":0}}}".to_string(),
        );
    }
    state.silent_recovery_requested = true;
    events
        .into_iter()
        .map(|event| {
            observe_anthropic_sse_block(&event, state);
            Bytes::from(format!("{event}\n\n"))
        })
        .collect()
}

fn observe_anthropic_sse_block(block: &str, state: &mut AnthropicTerminalState) {
    let named_event = block
        .lines()
        .find_map(|line| strip_sse_field(line, "event"))
        .map(str::trim);
    let data = block
        .lines()
        .filter_map(|line| strip_sse_field(line, "data"))
        .collect::<Vec<_>>()
        .join("\n");
    let parsed = serde_json::from_str::<Value>(&data).ok();
    let event_type = parsed
        .as_ref()
        .and_then(|value| value.get("type"))
        .and_then(Value::as_str)
        .or(named_event);

    match event_type {
        Some("message_start") => state.saw_message_start = true,
        Some("content_block_start") => {
            if let Some(index) = parsed
                .as_ref()
                .and_then(|value| value.get("index"))
                .and_then(Value::as_u64)
            {
                let block_type = parsed
                    .as_ref()
                    .and_then(|value| value.pointer("/content_block/type"))
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
                    .to_string();
                match block_type.as_str() {
                    "text" => state.saw_text_block = true,
                    // 已闭合的本地 tool_use 包含完整 JSON，可用 stop_reason=tool_use
                    // 安全提交；仍处于 open 状态时下面的 open-block 条件会拒绝修复。
                    "tool_use" => {
                        let initial_input = parsed
                            .as_ref()
                            .and_then(|value| value.pointer("/content_block/input"))
                            .filter(|input| {
                                !input.as_object().is_some_and(|object| object.is_empty())
                            })
                            .and_then(|input| serde_json::to_string(input).ok())
                            .unwrap_or_default();
                        state.tool_input_json.insert(index, initial_input);
                    }
                    // thinking/redacted_thinking 已闭合后不影响后续文本的安全收尾；
                    // open thinking 仍会因 open-block 类型不是 text 而被拒绝。
                    "thinking" | "redacted_thinking" => {}
                    // server_tool_use / web_search_tool_result / 未知扩展等不能伪造终止。
                    _ => {
                        state.open_unsafe_content_blocks.insert(index);
                    }
                }
                state.open_content_blocks.insert(index, block_type);
            }
        }
        Some("content_block_stop") => {
            if let Some(index) = parsed
                .as_ref()
                .and_then(|value| value.get("index"))
                .and_then(Value::as_u64)
            {
                if state.open_content_blocks.get(&index).map(String::as_str) == Some("tool_use") {
                    let input = state.tool_input_json.remove(&index).unwrap_or_default();
                    if input.is_empty() || serde_json::from_str::<Value>(&input).is_ok() {
                        state.saw_tool_use_block = true;
                    } else {
                        state.saw_invalid_non_text_block = true;
                    }
                }
                state.open_unsafe_content_blocks.remove(&index);
                state.open_content_blocks.remove(&index);
            }
        }
        Some("content_block_delta") => {
            let delta_type = parsed
                .as_ref()
                .and_then(|value| value.pointer("/delta/type"))
                .and_then(Value::as_str);
            if delta_type == Some("input_json_delta") {
                if let (Some(index), Some(partial_json)) = (
                    parsed
                        .as_ref()
                        .and_then(|value| value.get("index"))
                        .and_then(Value::as_u64),
                    parsed
                        .as_ref()
                        .and_then(|value| value.pointer("/delta/partial_json"))
                        .and_then(Value::as_str),
                ) {
                    state
                        .tool_input_json
                        .entry(index)
                        .or_default()
                        .push_str(partial_json);
                }
            }
            let has_text_delta = parsed
                .as_ref()
                .and_then(|value| value.pointer("/delta/type"))
                .and_then(Value::as_str)
                == Some("text_delta")
                && parsed
                    .as_ref()
                    .and_then(|value| value.pointer("/delta/text"))
                    .and_then(Value::as_str)
                    .is_some_and(|text| !text.is_empty());
            state.saw_text_delta |= has_text_delta;
        }
        Some("message_delta") => {
            state.saw_final_message_delta = parsed
                .as_ref()
                .and_then(|value| value.pointer("/delta/stop_reason"))
                .and_then(Value::as_str)
                .is_some_and(|reason| !reason.is_empty());
        }
        Some("message_stop") => state.saw_message_stop = true,
        Some("error") => {
            state.saw_error = true;
            let error_type = parsed
                .as_ref()
                .and_then(|value| value.pointer("/error/type"))
                .and_then(Value::as_str)
                .unwrap_or("stream_error");
            state.error_type = Some(error_type.to_string());
            state.error_status = Some(if error_type == "stream_timeout" {
                504
            } else {
                502
            });
            state.error_message = Some(
                parsed
                    .as_ref()
                    .and_then(|value| value.pointer("/error/message"))
                    .and_then(Value::as_str)
                    .unwrap_or("Upstream stream returned an error event")
                    .to_string(),
            );
        }
        _ => {}
    }
}

fn anthropic_can_complete_safely(state: &AnthropicTerminalState) -> bool {
    state.saw_message_start
        && state.saw_final_message_delta
        && state.open_content_blocks.is_empty()
        && !state.saw_error
}

fn anthropic_can_repair_truncated_turn(state: &AnthropicTerminalState) -> bool {
    state.saw_message_start
        && ((state.saw_text_block && state.saw_text_delta)
            || state.saw_tool_use_block
            || state.silent_recovery_requested)
        && !state.saw_invalid_non_text_block
        && state.open_unsafe_content_blocks.is_empty()
        && !state.saw_error
        && state
            .open_content_blocks
            .values()
            .all(|block_type| block_type == "text")
}

fn anthropic_terminal_chunks(
    state: &AnthropicTerminalState,
    error_type: &str,
    message: &str,
    allow_text_repair: bool,
) -> Vec<Bytes> {
    if state.saw_message_stop || state.saw_error {
        return Vec::new();
    }
    if anthropic_can_complete_safely(state) {
        return vec![Bytes::from_static(
            b"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
        )];
    }
    if allow_text_repair && anthropic_can_repair_truncated_turn(state) {
        let mut chunks = state
            .open_content_blocks
            .keys()
            .copied()
            .collect::<Vec<_>>();
        chunks.sort_unstable();
        let mut repaired = chunks
            .into_iter()
            .map(|index| {
                Bytes::from(format!(
                    "event: content_block_stop\ndata: {{\"type\":\"content_block_stop\",\"index\":{index}}}\n\n"
                ))
            })
            .collect::<Vec<_>>();
        let stop_reason = if state.saw_tool_use_block {
            "tool_use"
        } else {
            "max_tokens"
        };
        repaired.push(Bytes::from(format!(
            "event: message_delta\ndata: {{\"type\":\"message_delta\",\"delta\":{{\"stop_reason\":\"{stop_reason}\",\"stop_sequence\":null}},\"usage\":{{\"output_tokens\":0}}}}\n\n"
        )));
        repaired.push(Bytes::from_static(
            b"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
        ));
        return repaired;
    }
    let payload = serde_json::json!({
        "type": "error",
        "error": { "type": error_type, "message": message }
    });
    vec![Bytes::from(format!("event: error\ndata: {}\n\n", payload))]
}

pub fn create_logged_passthrough_stream(
    stream: impl Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static,
    tag: &'static str,
    usage_collector: Option<SseUsageCollector>,
    timeout_config: StreamingTimeoutConfig,
    connection_guard: Option<ActiveConnectionGuard>,
    terminal_policy: SseTerminalPolicy,
) -> impl Stream<Item = Result<Bytes, std::io::Error>> + Send {
    async_stream::stream! {
        let mut connection_guard = connection_guard;
        let mut buffer = String::new();
        let mut utf8_remainder: Vec<u8> = Vec::new();
        let mut collector = usage_collector;
        let mut finish_guard = collector.clone().map(SseUsageFinishGuard::new);
        let inspect_sse_events =
            collector.is_some()
                || log::log_enabled!(log::Level::Debug)
                || terminal_policy.is_anthropic();
        let mut anthropic_state = AnthropicTerminalState::default();
        let mut held_anthropic_blocks: HashMap<u64, HeldAnthropicContentBlock> = HashMap::new();
        let mut held_anthropic_bytes = 0usize;
        let mut ignored_held_indices = HashSet::new();
        let mut rewrote_incomplete_content = false;
        let mut suppressed_stream_error: Option<(String, String)> = None;
        let mut is_first_chunk = true;

        // 超时配置
        let first_byte_timeout = if timeout_config.first_byte_timeout > 0 {
            Some(Duration::from_secs(timeout_config.first_byte_timeout))
        } else {
            None
        };
        let idle_timeout = if timeout_config.idle_timeout > 0 {
            Some(Duration::from_secs(timeout_config.idle_timeout))
        } else {
            None
        };

        tokio::pin!(stream);

        'stream_loop: loop {
            // 选择超时时间：首字节超时或静默期超时
            let timeout_duration = if is_first_chunk {
                first_byte_timeout
            } else {
                idle_timeout
            };

            let chunk_result = match timeout_duration {
                Some(duration) => {
                    match tokio::time::timeout(duration, stream.next()).await {
                        Ok(Some(chunk)) => Some(chunk),
                        Ok(None) => None, // 流结束
                        Err(_) => {
                            // 超时
                            let timeout_type = if is_first_chunk { "首字节" } else { "静默期" };
                            log::error!("[{tag}] 流式响应{}超时 ({}秒)", timeout_type, duration.as_secs());
                            if !terminal_policy.is_anthropic()
                                || !anthropic_can_complete_safely(&anthropic_state)
                            {
                                if let Some(c) = &collector {
                                    c.set_terminal_failure(
                                        504,
                                        format!("Upstream stream was idle for {} seconds", duration.as_secs()),
                                    )
                                    .await;
                                }
                            }
                            if terminal_policy.is_anthropic() {
                                // 未形成完整 SSE block 的尾字节绝不能下发；否则补终止
                                // 事件之前 Claude 就可能先解析到半截 JSON/UTF-8 并报错。
                                buffer.clear();
                                utf8_remainder.clear();
                                let _ = discard_incomplete_anthropic_blocks(
                                    &mut held_anthropic_blocks,
                                    &mut anthropic_state,
                                );
                                let mut fallback_blocks = Vec::new();
                                if terminal_policy.is_claude_desktop() {
                                    fallback_blocks.extend(ensure_anthropic_silent_recovery(
                                        &mut anthropic_state,
                                    ));
                                }
                                let terminals = anthropic_terminal_chunks(
                                    &anthropic_state,
                                    "stream_timeout",
                                    &format!("Upstream stream was idle for {} seconds", duration.as_secs()),
                                    terminal_policy.is_claude_desktop(),
                                );
                                if terminal_policy.is_claude_desktop() {
                                    if let Some(guard) = connection_guard.as_mut() {
                                        guard
                                            .mark_stream_failure(&format!(
                                                "Upstream stream was idle for {} seconds",
                                                duration.as_secs()
                                            ))
                                            .await;
                                    }
                                }
                                for block in fallback_blocks {
                                    yield Ok(block);
                                }
                                for terminal in terminals {
                                    yield Ok(terminal);
                                }
                            } else {
                                yield Err(std::io::Error::other(format!("流式响应{timeout_type}超时")));
                            }
                            break;
                        }
                    }
                }
                None => stream.next().await, // 无超时限制
            };

            match chunk_result {
                Some(Ok(bytes)) => {
                    if is_first_chunk {
                        log::debug!(
                            "[{tag}] 已接收上游流式首包: bytes={}",
                            bytes.len()
                        );
                    }
                    is_first_chunk = false;
                    let mut completed_anthropic_blocks = Vec::new();
                    if inspect_sse_events {
                        crate::proxy::sse::append_utf8_safe(&mut buffer, &mut utf8_remainder, &bytes);

                        // 尝试解析并记录完整的 SSE 事件
                        while let Some(mut event_text) = take_sse_block(&mut buffer) {
                            if !event_text.trim().is_empty() {
                                if terminal_policy.is_anthropic() {
                                    let descriptor = describe_anthropic_sse_block(&event_text);
                                    let mut handled_as_held_block = false;

                                    if terminal_policy.is_claude_desktop()
                                        && descriptor.event_type == "content_block_start"
                                        && descriptor.block_type.as_deref() != Some("text")
                                    {
                                        if let (Some(index), Some(block_type)) =
                                            (descriptor.index, descriptor.block_type.clone())
                                        {
                                            let event_bytes = event_text.len();
                                            held_anthropic_bytes =
                                                held_anthropic_bytes.saturating_add(event_bytes);
                                            held_anthropic_blocks.insert(
                                                index,
                                                HeldAnthropicContentBlock {
                                                    block_type: block_type.clone(),
                                                    events: vec![event_text.clone()],
                                                    bytes: event_bytes,
                                                },
                                            );
                                            handled_as_held_block = true;
                                            if held_anthropic_bytes
                                                > MAX_HELD_ANTHROPIC_BLOCK_BYTES
                                            {
                                                let held = held_anthropic_blocks
                                                    .remove(&index)
                                                    .expect("held block exists");
                                                held_anthropic_bytes = held_anthropic_bytes
                                                    .saturating_sub(held.bytes);
                                                ignored_held_indices.insert(index);
                                                rewrote_incomplete_content = true;
                                                anthropic_state.silent_recovery_requested = true;
                                            }
                                        }
                                    } else if let Some(index) = descriptor.index {
                                        if ignored_held_indices.contains(&index) {
                                            handled_as_held_block = true;
                                            if descriptor.event_type == "content_block_stop" {
                                                ignored_held_indices.remove(&index);
                                            }
                                        } else if let Some(held) =
                                            held_anthropic_blocks.get_mut(&index)
                                        {
                                            let event_bytes = event_text.len();
                                            held.events.push(event_text.clone());
                                            held.bytes = held.bytes.saturating_add(event_bytes);
                                            held_anthropic_bytes =
                                                held_anthropic_bytes.saturating_add(event_bytes);
                                            handled_as_held_block = true;
                                            if held_anthropic_bytes
                                                > MAX_HELD_ANTHROPIC_BLOCK_BYTES
                                            {
                                                let held = held_anthropic_blocks
                                                    .remove(&index)
                                                    .expect("held block exists");
                                                held_anthropic_bytes = held_anthropic_bytes
                                                    .saturating_sub(held.bytes);
                                                if descriptor.event_type != "content_block_stop" {
                                                    ignored_held_indices.insert(index);
                                                }
                                                rewrote_incomplete_content = true;
                                                anthropic_state.silent_recovery_requested = true;
                                            } else if descriptor.event_type
                                                == "content_block_stop"
                                            {
                                                let held = held_anthropic_blocks
                                                    .remove(&index)
                                                    .expect("held block exists");
                                                held_anthropic_bytes = held_anthropic_bytes
                                                    .saturating_sub(held.bytes);
                                                let mut validation =
                                                    AnthropicTerminalState::default();
                                                for held_event in &held.events {
                                                    observe_anthropic_sse_block(
                                                        held_event,
                                                        &mut validation,
                                                    );
                                                }
                                                let invalid_tool = held.block_type == "tool_use"
                                                    && validation.saw_invalid_non_text_block;
                                                let events = if invalid_tool {
                                                    rewrote_incomplete_content = true;
                                                    anthropic_state.silent_recovery_requested = true;
                                                    Vec::new()
                                                } else {
                                                    held.events
                                                };
                                                for held_event in events {
                                                    observe_anthropic_sse_block(
                                                        &held_event,
                                                        &mut anthropic_state,
                                                    );
                                                    completed_anthropic_blocks.push(Bytes::from(
                                                        format!(
                                                            "{}\n\n",
                                                            held_event.trim_end_matches(|c| {
                                                                c == '\r' || c == '\n'
                                                            })
                                                        ),
                                                    ));
                                                }
                                            }
                                        }
                                    }

                                    if !handled_as_held_block {
                                        if matches!(
                                            descriptor.event_type.as_str(),
                                            "message_delta" | "message_stop" | "error"
                                        ) && !held_anthropic_blocks.is_empty()
                                        {
                                            rewrote_incomplete_content |=
                                                discard_incomplete_anthropic_blocks(
                                                    &mut held_anthropic_blocks,
                                                    &mut anthropic_state,
                                                );
                                            held_anthropic_bytes = 0;
                                            ignored_held_indices.clear();
                                        }
                                        let suppress_midstream_error = descriptor.event_type
                                            == "error"
                                            && terminal_policy.is_claude_desktop();
                                        if suppress_midstream_error {
                                            let mut error_state = AnthropicTerminalState::default();
                                            observe_anthropic_sse_block(
                                                &event_text,
                                                &mut error_state,
                                            );
                                            suppressed_stream_error = Some((
                                                error_state
                                                    .error_type
                                                    .unwrap_or_else(|| "stream_error".to_string()),
                                                error_state.error_message.unwrap_or_else(|| {
                                                    "Upstream stream returned an error event"
                                                        .to_string()
                                                }),
                                            ));
                                            rewrote_incomplete_content = true;
                                            if !anthropic_state.saw_text_delta {
                                                completed_anthropic_blocks.extend(
                                                    ensure_anthropic_silent_recovery(
                                                        &mut anthropic_state,
                                                    ),
                                                );
                                            }
                                            for terminal in anthropic_terminal_chunks(
                                                &anthropic_state,
                                                "stream_error",
                                                "Upstream stream failed after partial output",
                                                true,
                                            ) {
                                                if let Ok(terminal_text) =
                                                    std::str::from_utf8(&terminal)
                                                {
                                                    observe_anthropic_sse_block(
                                                        terminal_text,
                                                        &mut anthropic_state,
                                                    );
                                                }
                                                completed_anthropic_blocks.push(terminal);
                                            }
                                            event_text.clear();
                                        } else {
                                            if rewrote_incomplete_content
                                                && descriptor.event_type == "message_stop"
                                                && !anthropic_state.saw_final_message_delta
                                            {
                                                let repaired_delta = "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"max_tokens\",\"stop_sequence\":null},\"usage\":{\"output_tokens\":0}}";
                                                observe_anthropic_sse_block(
                                                    repaired_delta,
                                                    &mut anthropic_state,
                                                );
                                                completed_anthropic_blocks.push(Bytes::from(
                                                    format!("{repaired_delta}\n\n"),
                                                ));
                                            }
                                            if rewrote_incomplete_content
                                                && descriptor.event_type == "message_delta"
                                            {
                                                event_text = "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"max_tokens\",\"stop_sequence\":null},\"usage\":{\"output_tokens\":0}}".to_string();
                                            }
                                            observe_anthropic_sse_block(
                                                &event_text,
                                                &mut anthropic_state,
                                            );
                                            completed_anthropic_blocks.push(Bytes::from(format!(
                                                "{}\n\n",
                                                event_text.trim_end_matches(|c| {
                                                    c == '\r' || c == '\n'
                                                })
                                            )));
                                        }
                                    }
                                }
                                // 提取 data 部分；只有 usage collector 存在时才解析 JSON。
                                for line in event_text.lines() {
                                    if let Some(data) = strip_sse_field(line, "data") {
                                        if data.trim() != "[DONE]" {
                                            let collected = match &collector {
                                                Some(c) if c.should_collect(data) => {
                                                    match serde_json::from_str::<Value>(data) {
                                                        Ok(json_value) => {
                                                            c.push(json_value).await;
                                                            true
                                                        }
                                                        Err(_) => false,
                                                    }
                                                }
                                                _ => false,
                                            };
                                            log::trace!(
                                                "[{tag}] <<< SSE data: bytes={}, usage_collected={collected} (content omitted)",
                                                data.len()
                                            );
                                        } else {
                                            log::debug!("[{tag}] <<< SSE: [DONE]");
                                        }
                                    }
                                }
                            }
                        }

                        // 先 drain 所有完整 SSE block，再限制剩余的“未分帧单事件”。
                        // 否则一个大 chunk 中前面的合法 message_start/text 也会被误丢。
                        if terminal_policy.is_anthropic()
                            && buffer.len().saturating_add(utf8_remainder.len())
                                > MAX_UNFRAMED_ANTHROPIC_SSE_BYTES
                        {
                            log::error!(
                                "[{tag}] Anthropic SSE 单事件超过 {} bytes 且未完成分帧",
                                MAX_UNFRAMED_ANTHROPIC_SSE_BYTES
                            );
                            buffer.clear();
                            utf8_remainder.clear();
                            let _ = discard_incomplete_anthropic_blocks(
                                &mut held_anthropic_blocks,
                                &mut anthropic_state,
                            );
                            let mut recovery_blocks = Vec::new();
                            if terminal_policy.is_claude_desktop() {
                                recovery_blocks.extend(ensure_anthropic_silent_recovery(
                                    &mut anthropic_state,
                                ));
                            }
                            let terminals = anthropic_terminal_chunks(
                                &anthropic_state,
                                "stream_truncated",
                                "Upstream SSE event exceeded the framing limit",
                                terminal_policy.is_claude_desktop(),
                            );
                            if let Some(c) = &collector {
                                c.set_terminal_failure(
                                    502,
                                    "Upstream SSE event exceeded the framing limit".to_string(),
                                )
                                .await;
                            }
                            if terminal_policy.is_claude_desktop() {
                                if let Some(guard) = connection_guard.as_mut() {
                                    guard
                                        .mark_stream_failure(
                                            "Upstream SSE event exceeded the framing limit",
                                        )
                                        .await;
                                }
                            }
                            for block in completed_anthropic_blocks.drain(..) {
                                yield Ok(block);
                            }
                            for block in recovery_blocks {
                                yield Ok(block);
                            }
                            for terminal in terminals {
                                yield Ok(terminal);
                            }
                            break 'stream_loop;
                        }
                    }

                    let anthropic_terminal = terminal_policy.is_anthropic()
                        && (anthropic_state.saw_message_stop || anthropic_state.saw_error);
                    if terminal_policy.is_claude_desktop() && anthropic_terminal {
                        if let Some((error_type, message)) = suppressed_stream_error.take() {
                            if let Some(guard) = connection_guard.as_mut() {
                                guard.note_stream_error(&error_type, &message).await;
                            }
                            if let Some(c) = &collector {
                                c.set_terminal_failure(502, message).await;
                            }
                        } else if anthropic_state.saw_error {
                            if let Some(guard) = connection_guard.as_mut() {
                                guard
                                    .note_stream_error(
                                        anthropic_state
                                            .error_type
                                            .as_deref()
                                            .unwrap_or("stream_error"),
                                        anthropic_state
                                            .error_message
                                            .as_deref()
                                            .unwrap_or("Upstream stream returned an error event"),
                                    )
                                    .await;
                            }
                        } else if anthropic_can_complete_safely(&anthropic_state)
                            && !rewrote_incomplete_content
                        {
                            if let Some(guard) = connection_guard.as_mut() {
                                guard.mark_stream_success().await;
                            }
                        } else {
                            const MESSAGE_STOP_REPAIR_FAILURE: &str =
                                "Upstream emitted message_stop before a complete Anthropic turn";
                            if let Some(guard) = connection_guard.as_mut() {
                                guard.mark_stream_failure(MESSAGE_STOP_REPAIR_FAILURE).await;
                            }
                            if let Some(c) = &collector {
                                c.set_terminal_failure(502, MESSAGE_STOP_REPAIR_FAILURE)
                                    .await;
                            }
                        }
                    }

                    if terminal_policy.is_anthropic() {
                        for block in completed_anthropic_blocks {
                            yield Ok(block);
                        }
                    } else {
                        yield Ok(bytes);
                    }
                    if anthropic_state.saw_error {
                        if let Some(c) = &collector {
                            c.set_terminal_failure(
                                anthropic_state.error_status.unwrap_or(502),
                                anthropic_state
                                    .error_message
                                    .clone()
                                    .unwrap_or_else(|| "Upstream stream returned an error event".to_string()),
                            )
                            .await;
                        }
                    }
                    if terminal_policy.is_anthropic()
                        && (anthropic_state.saw_message_stop || anthropic_state.saw_error)
                    {
                        break;
                    }
                }
                Some(Err(e)) => {
                    log::error!("[{tag}] 流错误: {e}");
                    if !terminal_policy.is_anthropic()
                        || !anthropic_can_complete_safely(&anthropic_state)
                    {
                        if let Some(c) = &collector {
                            c.set_terminal_failure(502, format!("Upstream stream I/O error: {e}"))
                                .await;
                        }
                    }
                    if terminal_policy.is_anthropic() {
                        buffer.clear();
                        utf8_remainder.clear();
                        let _ = discard_incomplete_anthropic_blocks(
                            &mut held_anthropic_blocks,
                            &mut anthropic_state,
                        );
                        let mut fallback_blocks = Vec::new();
                        if terminal_policy.is_claude_desktop() {
                            fallback_blocks.extend(ensure_anthropic_silent_recovery(
                                &mut anthropic_state,
                            ));
                        }
                        let terminals = anthropic_terminal_chunks(
                            &anthropic_state,
                            "stream_error",
                            "Upstream stream ended with an I/O error",
                            terminal_policy.is_claude_desktop(),
                        );
                        if terminal_policy.is_claude_desktop() {
                            if let Some(guard) = connection_guard.as_mut() {
                                guard
                                    .mark_stream_failure("Upstream stream ended with an I/O error")
                                    .await;
                            }
                        }
                        for block in fallback_blocks {
                            yield Ok(block);
                        }
                        for terminal in terminals {
                            yield Ok(terminal);
                        }
                    } else {
                        yield Err(std::io::Error::other(e.to_string()));
                    }
                    break;
                }
                None => {
                    if terminal_policy.is_anthropic() {
                        buffer.clear();
                        utf8_remainder.clear();
                        rewrote_incomplete_content |= discard_incomplete_anthropic_blocks(
                            &mut held_anthropic_blocks,
                            &mut anthropic_state,
                        );
                        let mut fallback_blocks = Vec::new();
                        if terminal_policy.is_claude_desktop()
                            && !anthropic_can_complete_safely(&anthropic_state)
                        {
                            fallback_blocks.extend(ensure_anthropic_silent_recovery(
                                &mut anthropic_state,
                            ));
                        }
                        let terminals = anthropic_terminal_chunks(
                            &anthropic_state,
                            "stream_truncated",
                            "Upstream stream ended before message_stop",
                            true,
                        );
                        if terminal_policy.is_claude_desktop()
                            && anthropic_can_complete_safely(&anthropic_state)
                            && !rewrote_incomplete_content
                        {
                            if let Some(guard) = connection_guard.as_mut() {
                                guard.mark_stream_success().await;
                            }
                        } else if terminal_policy.is_claude_desktop() {
                            if let Some(guard) = connection_guard.as_mut() {
                                guard
                                    .mark_stream_failure(
                                        "Upstream stream ended before message_stop",
                                    )
                                    .await;
                            }
                        }
                        if rewrote_incomplete_content
                            || !anthropic_can_complete_safely(&anthropic_state)
                        {
                            if let Some(c) = &collector {
                                c.set_terminal_failure(
                                    502,
                                    "Upstream stream ended before message_stop",
                                )
                                .await;
                            }
                        }
                        for block in fallback_blocks {
                            yield Ok(block);
                        }
                        for terminal in terminals {
                            yield Ok(terminal);
                        }
                    }
                    break;
                }
            }
        }

        if let Some(c) = collector.take() {
            c.finish().await;
        }
        if let Some(guard) = &mut finish_guard {
            guard.disarm();
        }
    }
}

fn is_safe_diagnostic_header(name: &str) -> bool {
    matches!(
        name,
        "content-type"
            | "content-encoding"
            | "content-length"
            | "retry-after"
            | "cf-ray"
            | "x-request-id"
            | "request-id"
            | "x-correlation-id"
    ) || name.starts_with("x-ratelimit-")
        || name.starts_with("ratelimit-")
}

fn bounded_header_value(value: &axum::http::HeaderValue) -> Option<String> {
    let value = value.to_str().ok()?;
    let mut bounded = value.chars().take(160).collect::<String>();
    if value.chars().count() > 160 {
        bounded.push('…');
    }
    Some(bounded)
}

fn format_headers(headers: &HeaderMap) -> String {
    let mut entries = headers
        .keys()
        .map(|key| {
            let name = key.as_str();
            if !is_safe_diagnostic_header(name) {
                return name.to_string();
            }

            let values = headers
                .get_all(key)
                .iter()
                .filter_map(bounded_header_value)
                .collect::<Vec<_>>();
            if values.is_empty() {
                name.to_string()
            } else {
                format!("{name}={}", values.join("|"))
            }
        })
        .collect::<Vec<_>>();
    entries.sort();
    format!("[{}]", entries.join(", "))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::Database;
    use crate::error::AppError;
    use crate::provider::ProviderMeta;
    use crate::proxy::failover_switch::FailoverSwitchManager;
    use crate::proxy::provider_router::ProviderRouter;
    use crate::proxy::providers::{
        codex_chat_history::CodexChatHistoryStore, gemini_shadow::GeminiShadowStore,
    };
    use crate::proxy::types::{ProxyConfig, ProxyStatus};
    use rust_decimal::Decimal;
    use std::collections::HashMap;
    use std::str::FromStr;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    fn terminal_outcome_collector() -> (
        SseUsageCollector,
        Arc<std::sync::Mutex<Option<StreamTerminalOutcome>>>,
    ) {
        let captured = Arc::new(std::sync::Mutex::new(None));
        let sink = Arc::clone(&captured);
        let collector = SseUsageCollector::new(
            std::time::Instant::now(),
            None,
            move |_, _, terminal_outcome| {
                *sink.lock().expect("terminal outcome lock") = Some(terminal_outcome);
            },
        );
        (collector, captured)
    }

    #[test]
    fn format_headers_keeps_only_allowlisted_diagnostic_values() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer super-secret".parse().unwrap());
        headers.insert("set-cookie", "session=cookie-secret".parse().unwrap());
        headers.insert("retry-after", "30".parse().unwrap());
        headers.insert("x-ratelimit-remaining", "2".parse().unwrap());
        headers.insert("cf-ray", "abc123-SJC".parse().unwrap());

        let formatted = format_headers(&headers);
        assert!(formatted.contains("authorization"), "{formatted}");
        assert!(formatted.contains("set-cookie"), "{formatted}");
        assert!(formatted.contains("retry-after=30"), "{formatted}");
        assert!(formatted.contains("x-ratelimit-remaining=2"), "{formatted}");
        assert!(formatted.contains("cf-ray=abc123-SJC"), "{formatted}");
        assert!(!formatted.contains("super-secret"), "{formatted}");
        assert!(!formatted.contains("cookie-secret"), "{formatted}");
    }

    #[tokio::test]
    async fn read_decoded_body_rejects_compressed_bomb_without_full_expansion() {
        // 128 MiB+1 全零 payload 的 gzip 只有 ~130 KiB：原始读取上限拦不住它，
        // 只有解压侧的有界解码能拒绝。若解码退化为"先完整展开再比较"，
        // 展开后长度 > MAX_RESPONSE_BODY_BYTES 的 payload 会成功返回（测试失败）。
        let payload = vec![0u8; MAX_RESPONSE_BODY_BYTES + 1];
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        std::io::Write::write_all(&mut encoder, &payload).unwrap();
        let compressed = encoder.finish().unwrap();
        assert!(compressed.len() < MAX_RESPONSE_BODY_BYTES);

        let mut headers = HeaderMap::new();
        headers.insert("content-encoding", "gzip".parse().unwrap());
        let response =
            ProxyResponse::buffered(http::StatusCode::OK, headers, Bytes::from(compressed));

        let result = read_decoded_body(response, "test", Duration::ZERO).await;
        assert!(
            matches!(result, Err(ProxyError::ResponseBodyTooLarge(_))),
            "压缩炸弹应被拒绝而不是完整展开: {:?}",
            result.map(|(_, _, body)| body.len())
        );
    }

    #[test]
    fn test_strip_sse_field_accepts_optional_space() {
        assert_eq!(
            super::strip_sse_field("data: {\"ok\":true}", "data"),
            Some("{\"ok\":true}")
        );
        assert_eq!(
            super::strip_sse_field("data:{\"ok\":true}", "data"),
            Some("{\"ok\":true}")
        );
        assert_eq!(
            super::strip_sse_field("event: message_start", "event"),
            Some("message_start")
        );
        assert_eq!(
            super::strip_sse_field("event:message_start", "event"),
            Some("message_start")
        );
        assert_eq!(super::strip_sse_field("id:1", "data"), None);
    }

    #[tokio::test]
    async fn anthropic_clean_eof_without_terminal_emits_protocol_error() {
        let outcome = Arc::new(std::sync::Mutex::new(None));
        let outcome_for_callback = outcome.clone();
        let collector =
            SseUsageCollector::new(std::time::Instant::now(), None, move |_, _, terminal| {
                *outcome_for_callback.lock().expect("outcome lock") = Some(terminal);
            });
        let source = futures::stream::iter(vec![Ok::<Bytes, std::io::Error>(Bytes::from_static(
            b"event: message_start\ndata: {\"type\":\"message_start\"}\n\nevent: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"partial\"}}\n\n",
        ))]);
        let output = create_logged_passthrough_stream(
            source,
            "test",
            Some(collector),
            StreamingTimeoutConfig {
                first_byte_timeout: 0,
                idle_timeout: 0,
            },
            None,
            SseTerminalPolicy::AnthropicMessages,
        )
        .collect::<Vec<_>>()
        .await;
        assert!(output.iter().all(Result::is_ok));
        let text = output
            .into_iter()
            .map(Result::unwrap)
            .fold(Vec::new(), |mut all, bytes| {
                all.extend_from_slice(&bytes);
                all
            });
        let text = String::from_utf8(text).unwrap();
        assert!(text.contains("event: error"), "{text}");
        assert!(text.contains("stream_truncated"), "{text}");
        assert!(!text.contains("event: message_stop"), "{text}");
        let terminal = outcome
            .lock()
            .expect("outcome lock")
            .clone()
            .expect("terminal outcome");
        assert_eq!(terminal.status_code_override, Some(502));
        assert_eq!(
            terminal.error_message.as_deref(),
            Some("Upstream stream ended before message_stop")
        );
    }

    #[tokio::test]
    async fn anthropic_io_error_is_encoded_as_sse_not_body_error() {
        let source = futures::stream::iter(vec![Err::<Bytes, std::io::Error>(
            std::io::Error::other("disconnected"),
        )]);
        let output = create_logged_passthrough_stream(
            source,
            "test",
            None,
            StreamingTimeoutConfig {
                first_byte_timeout: 0,
                idle_timeout: 0,
            },
            None,
            SseTerminalPolicy::AnthropicMessages,
        )
        .collect::<Vec<_>>()
        .await;
        assert!(output.iter().all(Result::is_ok));
        let text = output
            .into_iter()
            .map(Result::unwrap)
            .fold(Vec::new(), |mut all, bytes| {
                all.extend_from_slice(&bytes);
                all
            });
        let text = String::from_utf8(text).unwrap();
        assert!(text.contains("event: error"), "{text}");
        assert!(text.contains("stream_error"), "{text}");
    }

    #[tokio::test]
    async fn anthropic_truncated_pure_text_is_closed_with_max_tokens() {
        let source = futures::stream::iter(vec![Ok::<Bytes, std::io::Error>(Bytes::from_static(
            b"event: message_start\ndata: {\"type\":\"message_start\"}\n\nevent: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"partial\"}}\n\n",
        ))]);
        let output = create_logged_passthrough_stream(
            source,
            "test",
            None,
            StreamingTimeoutConfig {
                first_byte_timeout: 0,
                idle_timeout: 0,
            },
            None,
            SseTerminalPolicy::AnthropicMessages,
        )
        .collect::<Vec<_>>()
        .await;
        assert!(output.iter().all(Result::is_ok));
        let text = output
            .into_iter()
            .map(Result::unwrap)
            .fold(Vec::new(), |mut all, bytes| {
                all.extend_from_slice(&bytes);
                all
            });
        let text = String::from_utf8(text).unwrap();
        assert!(text.contains("event: content_block_stop"), "{text}");
        assert!(text.contains("\"stop_reason\":\"max_tokens\""), "{text}");
        assert!(text.contains("event: message_stop"), "{text}");
        assert!(!text.contains("event: error"), "{text}");
    }

    #[tokio::test]
    async fn anthropic_closed_thinking_then_partial_text_is_repaired() {
        let source = futures::stream::iter(vec![Ok::<Bytes, std::io::Error>(Bytes::from_static(
            b"event: message_start\ndata: {\"type\":\"message_start\"}\n\nevent: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"thinking\",\"thinking\":\"\"}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"reason\"}}\n\nevent: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\nevent: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"text_delta\",\"text\":\"partial\"}}\n\n",
        ))]);
        let output = create_logged_passthrough_stream(
            source,
            "test",
            None,
            StreamingTimeoutConfig {
                first_byte_timeout: 0,
                idle_timeout: 0,
            },
            None,
            SseTerminalPolicy::AnthropicMessages,
        )
        .collect::<Vec<_>>()
        .await;
        let text = output
            .into_iter()
            .map(Result::unwrap)
            .fold(Vec::new(), |mut all, bytes| {
                all.extend_from_slice(&bytes);
                all
            });
        let text = String::from_utf8(text).unwrap();
        assert!(text.contains("\"stop_reason\":\"max_tokens\""), "{text}");
        assert!(text.contains("event: message_stop"), "{text}");
        assert!(!text.contains("event: error"), "{text}");
    }

    #[tokio::test]
    async fn anthropic_closed_tool_use_is_repaired_with_tool_stop_reason() {
        let source = futures::stream::iter(vec![Ok::<Bytes, std::io::Error>(Bytes::from_static(
            b"event: message_start\ndata: {\"type\":\"message_start\"}\n\nevent: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"tool_1\",\"name\":\"Bash\",\"input\":{}}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"cmd\\\":\\\"pwd\\\"}\"}}\n\nevent: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
        ))]);
        let output = create_logged_passthrough_stream(
            source,
            "test",
            None,
            StreamingTimeoutConfig {
                first_byte_timeout: 0,
                idle_timeout: 0,
            },
            None,
            SseTerminalPolicy::AnthropicMessages,
        )
        .collect::<Vec<_>>()
        .await;
        let text = output
            .into_iter()
            .map(Result::unwrap)
            .fold(Vec::new(), |mut all, bytes| {
                all.extend_from_slice(&bytes);
                all
            });
        let text = String::from_utf8(text).unwrap();
        assert!(text.contains("\"stop_reason\":\"tool_use\""), "{text}");
        assert!(text.contains("event: message_stop"), "{text}");
        assert!(!text.contains("event: error"), "{text}");
    }

    #[tokio::test]
    async fn anthropic_closed_tool_use_with_invalid_json_is_not_forged() {
        let invalid_delta = serde_json::json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "input_json_delta", "partial_json": "{\"cmd\":"}
        });
        let source = format!(
            "event: message_start\ndata: {{\"type\":\"message_start\"}}\n\nevent: content_block_start\ndata: {{\"type\":\"content_block_start\",\"index\":0,\"content_block\":{{\"type\":\"tool_use\",\"id\":\"tool_1\",\"name\":\"Bash\",\"input\":{{}}}}}}\n\nevent: content_block_delta\ndata: {invalid_delta}\n\nevent: content_block_stop\ndata: {{\"type\":\"content_block_stop\",\"index\":0}}\n\n"
        );
        let output = create_logged_passthrough_stream(
            futures::stream::iter(vec![Ok::<Bytes, std::io::Error>(Bytes::from(source))]),
            "test",
            None,
            StreamingTimeoutConfig {
                first_byte_timeout: 0,
                idle_timeout: 0,
            },
            None,
            SseTerminalPolicy::AnthropicMessages,
        )
        .collect::<Vec<_>>()
        .await;
        let text = output
            .into_iter()
            .map(Result::unwrap)
            .fold(Vec::new(), |mut all, bytes| {
                all.extend_from_slice(&bytes);
                all
            });
        let text = String::from_utf8(text).unwrap();
        assert!(text.contains("event: error"), "{text}");
        assert!(!text.contains("\"stop_reason\":\"tool_use\""), "{text}");
    }

    #[tokio::test]
    async fn anthropic_partial_sse_tail_is_dropped_before_terminal_repair() {
        let source = futures::stream::iter(vec![
            Ok::<Bytes, std::io::Error>(Bytes::from_static(
                b"event: message_start\ndata: {\"type\":\"message_start\"}\n\nevent: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"safe\"}}\n\n",
            )),
            Ok(Bytes::from_static(
                b"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"BROKEN_TAIL",
            )),
        ]);
        let output = create_logged_passthrough_stream(
            source,
            "test",
            None,
            StreamingTimeoutConfig {
                first_byte_timeout: 0,
                idle_timeout: 0,
            },
            None,
            SseTerminalPolicy::ClaudeDesktopAnthropic,
        )
        .collect::<Vec<_>>()
        .await;
        let text = output
            .into_iter()
            .map(Result::unwrap)
            .fold(Vec::new(), |mut all, bytes| {
                all.extend_from_slice(&bytes);
                all
            });
        let text = String::from_utf8(text).unwrap();
        assert!(text.contains("\"text\":\"safe\""), "{text}");
        assert!(
            !text.contains("BROKEN_TAIL"),
            "incomplete tail leaked: {text}"
        );
        assert!(text.contains("event: message_stop"), "{text}");
        assert!(!text.contains("event: error"), "{text}");
    }

    #[tokio::test]
    async fn anthropic_open_tool_use_is_never_forged_as_complete() {
        let source = futures::stream::iter(vec![Ok::<Bytes, std::io::Error>(Bytes::from_static(
            b"event: message_start\ndata: {\"type\":\"message_start\"}\n\nevent: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"tool_1\",\"name\":\"Bash\",\"input\":{}}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"cmd\\\":\"\"}}\n\n",
        ))]);
        let output = create_logged_passthrough_stream(
            source,
            "test",
            None,
            StreamingTimeoutConfig {
                first_byte_timeout: 0,
                idle_timeout: 0,
            },
            None,
            SseTerminalPolicy::AnthropicMessages,
        )
        .collect::<Vec<_>>()
        .await;
        let text = output
            .into_iter()
            .map(Result::unwrap)
            .fold(Vec::new(), |mut all, bytes| {
                all.extend_from_slice(&bytes);
                all
            });
        let text = String::from_utf8(text).unwrap();
        assert!(text.contains("event: error"), "{text}");
        assert!(!text.contains("\"stop_reason\":\"tool_use\""), "{text}");
    }

    #[tokio::test]
    async fn claude_desktop_text_then_open_tool_is_rewritten_without_api_error() {
        let source = futures::stream::iter(vec![Ok::<Bytes, std::io::Error>(Bytes::from(
            "event: message_start\ndata: {\"type\":\"message_start\"}\n\nevent: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"先检查一下。\"}}\n\nevent: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\nevent: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"tool_1\",\"name\":\"Bash\",\"input\":{}}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"cmd\\\":\"}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
        ))]);
        let output = create_logged_passthrough_stream(
            source,
            "test",
            None,
            StreamingTimeoutConfig {
                first_byte_timeout: 0,
                idle_timeout: 0,
            },
            None,
            SseTerminalPolicy::ClaudeDesktopAnthropic,
        )
        .collect::<Vec<_>>()
        .await;
        let text = output
            .into_iter()
            .map(Result::unwrap)
            .fold(Vec::new(), |mut all, bytes| {
                all.extend_from_slice(&bytes);
                all
            });
        let text = String::from_utf8(text).unwrap();
        assert!(text.contains("先检查一下。"), "{text}");
        assert!(!text.contains("CC Switch"), "{text}");
        assert!(!text.contains("安全取消"), "{text}");
        assert!(!text.contains("自动续试"), "{text}");
        assert!(!text.contains("\"type\":\"tool_use\""), "{text}");
        assert!(text.contains("\"stop_reason\":\"max_tokens\""), "{text}");
        assert!(text.contains("event: message_stop"), "{text}");
        assert!(!text.contains("event: error"), "{text}");
    }

    #[tokio::test]
    async fn claude_desktop_dropped_tool_use_rewrites_upstream_tool_stop_reason() {
        let source = futures::stream::iter(vec![Ok::<Bytes, std::io::Error>(Bytes::from(
            "event: message_start\ndata: {\"type\":\"message_start\"}\n\nevent: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"kept\"}}\n\nevent: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\nevent: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"tool_1\",\"name\":\"Bash\",\"input\":{}}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"cmd\\\":\"}}\n\nevent: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\",\"stop_sequence\":null},\"usage\":{\"output_tokens\":1}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
        ))]);
        let (collector, captured_outcome) = terminal_outcome_collector();
        let output = create_logged_passthrough_stream(
            source,
            "test",
            Some(collector),
            StreamingTimeoutConfig {
                first_byte_timeout: 0,
                idle_timeout: 0,
            },
            None,
            SseTerminalPolicy::ClaudeDesktopAnthropic,
        )
        .collect::<Vec<_>>()
        .await;
        let text = output
            .into_iter()
            .map(Result::unwrap)
            .fold(Vec::new(), |mut all, bytes| {
                all.extend_from_slice(&bytes);
                all
            });
        let text = String::from_utf8(text).unwrap();
        assert!(text.contains("kept"), "{text}");
        assert!(!text.contains("\"type\":\"tool_use\""), "{text}");
        assert!(!text.contains("\"stop_reason\":\"tool_use\""), "{text}");
        assert!(text.contains("\"stop_reason\":\"max_tokens\""), "{text}");
        assert_eq!(text.matches("event: message_stop").count(), 1, "{text}");
        assert!(!text.contains("event: error"), "{text}");
        let outcome = captured_outcome
            .lock()
            .expect("captured outcome lock")
            .clone()
            .expect("terminal outcome");
        assert_eq!(outcome.status_code_override, Some(502));
        assert!(outcome
            .error_message
            .as_deref()
            .is_some_and(|message| message.contains("message_stop")));
    }

    #[tokio::test]
    async fn claude_desktop_eof_after_dropped_tool_marks_log_failure() {
        let source = futures::stream::iter(vec![Ok::<Bytes, std::io::Error>(Bytes::from(
            "event: message_start\ndata: {\"type\":\"message_start\"}\n\nevent: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"kept\"}}\n\nevent: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\nevent: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"tool_1\",\"name\":\"Bash\",\"input\":{}}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"cmd\\\":\"}}\n\n",
        ))]);
        let (collector, captured_outcome) = terminal_outcome_collector();
        let output = create_logged_passthrough_stream(
            source,
            "test",
            Some(collector),
            StreamingTimeoutConfig {
                first_byte_timeout: 0,
                idle_timeout: 0,
            },
            None,
            SseTerminalPolicy::ClaudeDesktopAnthropic,
        )
        .collect::<Vec<_>>()
        .await;
        let text = output
            .into_iter()
            .map(Result::unwrap)
            .fold(Vec::new(), |mut all, bytes| {
                all.extend_from_slice(&bytes);
                all
            });
        let text = String::from_utf8(text).unwrap();
        assert!(text.contains("kept"), "{text}");
        assert!(!text.contains("\"type\":\"tool_use\""), "{text}");
        assert!(text.contains("\"stop_reason\":\"max_tokens\""), "{text}");
        assert!(text.contains("event: message_stop"), "{text}");
        let outcome = captured_outcome
            .lock()
            .expect("captured outcome lock")
            .clone()
            .expect("terminal outcome");
        assert_eq!(outcome.status_code_override, Some(502));
        assert!(outcome
            .error_message
            .as_deref()
            .is_some_and(|message| message.contains("before message_stop")));
    }

    #[tokio::test]
    async fn claude_desktop_midstream_error_preserves_text_as_completed_turn() {
        let source = futures::stream::iter(vec![Ok::<Bytes, std::io::Error>(Bytes::from_static(
            b"event: message_start\ndata: {\"type\":\"message_start\"}\n\nevent: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"partial answer\"}}\n\nevent: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"rate_limit_error\",\"message\":\"Please retry later\"}}\n\n",
        ))]);
        let output = create_logged_passthrough_stream(
            source,
            "test",
            None,
            StreamingTimeoutConfig {
                first_byte_timeout: 0,
                idle_timeout: 0,
            },
            None,
            SseTerminalPolicy::ClaudeDesktopAnthropic,
        )
        .collect::<Vec<_>>()
        .await;
        let text = output
            .into_iter()
            .map(Result::unwrap)
            .fold(Vec::new(), |mut all, bytes| {
                all.extend_from_slice(&bytes);
                all
            });
        let text = String::from_utf8(text).unwrap();
        assert!(text.contains("partial answer"), "{text}");
        assert!(text.contains("\"stop_reason\":\"max_tokens\""), "{text}");
        assert!(text.contains("event: message_stop"), "{text}");
        assert!(!text.contains("event: error"), "{text}");
    }

    #[tokio::test]
    async fn claude_desktop_error_after_thinking_becomes_continuable_turn() {
        let source = futures::stream::iter(vec![Ok::<Bytes, std::io::Error>(Bytes::from_static(
            b"event: message_start\ndata: {\"type\":\"message_start\"}\n\nevent: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"thinking\",\"thinking\":\"\"}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"reasoning\"}}\n\nevent: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\nevent: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"rate_limit_error\",\"message\":\"Please retry later\"}}\n\n",
        ))]);
        let output = create_logged_passthrough_stream(
            source,
            "test",
            None,
            StreamingTimeoutConfig {
                first_byte_timeout: 0,
                idle_timeout: 0,
            },
            None,
            SseTerminalPolicy::ClaudeDesktopAnthropic,
        )
        .collect::<Vec<_>>()
        .await;
        let text = output
            .into_iter()
            .map(Result::unwrap)
            .fold(Vec::new(), |mut all, bytes| {
                all.extend_from_slice(&bytes);
                all
            });
        let text = String::from_utf8(text).unwrap();
        assert!(text.contains("reasoning"), "{text}");
        assert!(!text.contains("CC Switch"), "{text}");
        assert!(!text.contains("安全取消"), "{text}");
        assert!(!text.contains("自动续试"), "{text}");
        assert!(text.contains("\"stop_reason\":\"max_tokens\""), "{text}");
        assert!(text.contains("event: message_stop"), "{text}");
        assert!(!text.contains("event: error"), "{text}");
    }

    #[tokio::test]
    async fn claude_desktop_error_before_message_start_is_silent_and_continuable() {
        let source = futures::stream::iter(vec![Ok::<Bytes, std::io::Error>(Bytes::from_static(
            b"event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"rate_limit_error\",\"message\":\"Please retry later\"}}\n\n",
        ))]);
        let output = create_logged_passthrough_stream(
            source,
            "test",
            None,
            StreamingTimeoutConfig {
                first_byte_timeout: 0,
                idle_timeout: 0,
            },
            None,
            SseTerminalPolicy::ClaudeDesktopAnthropic,
        )
        .collect::<Vec<_>>()
        .await;
        let text = output
            .into_iter()
            .map(Result::unwrap)
            .fold(Vec::new(), |mut all, bytes| {
                all.extend_from_slice(&bytes);
                all
            });
        let text = String::from_utf8(text).unwrap();
        assert!(text.contains("event: message_start"), "{text}");
        assert!(text.contains("\"stop_reason\":\"max_tokens\""), "{text}");
        assert!(text.contains("event: message_stop"), "{text}");
        assert!(!text.contains("event: error"), "{text}");
        assert!(!text.contains("text_delta"), "{text}");
        assert!(!text.contains("content_block_start"), "{text}");
        assert!(!text.contains("content_block_stop"), "{text}");
        assert!(!text.contains("CC Switch"), "{text}");
        assert!(!text.contains("自动续试"), "{text}");
        assert!(!text.contains("安全取消"), "{text}");
    }

    #[tokio::test]
    async fn claude_desktop_eof_before_message_start_is_silent_and_continuable() {
        let output = create_logged_passthrough_stream(
            futures::stream::empty::<Result<Bytes, std::io::Error>>(),
            "test",
            None,
            StreamingTimeoutConfig {
                first_byte_timeout: 0,
                idle_timeout: 0,
            },
            None,
            SseTerminalPolicy::ClaudeDesktopAnthropic,
        )
        .collect::<Vec<_>>()
        .await;
        let text = output
            .into_iter()
            .map(Result::unwrap)
            .fold(Vec::new(), |mut all, bytes| {
                all.extend_from_slice(&bytes);
                all
            });
        let text = String::from_utf8(text).unwrap();
        assert!(text.contains("event: message_start"), "{text}");
        assert!(text.contains("\"stop_reason\":\"max_tokens\""), "{text}");
        assert!(text.contains("event: message_stop"), "{text}");
        assert!(!text.contains("event: error"), "{text}");
        assert!(!text.contains("text_delta"), "{text}");
        assert!(!text.contains("content_block_start"), "{text}");
        assert!(!text.contains("content_block_stop"), "{text}");
        assert!(!text.contains("CC Switch"), "{text}");
    }

    #[tokio::test]
    async fn claude_desktop_caps_held_tool_memory_and_falls_back() {
        let oversized = "x".repeat(MAX_HELD_ANTHROPIC_BLOCK_BYTES + 1);
        let delta = serde_json::json!({
            "type": "content_block_delta",
            "index": 1,
            "delta": {"type": "input_json_delta", "partial_json": oversized}
        });
        let source = format!(
            "event: message_start\ndata: {{\"type\":\"message_start\"}}\n\nevent: content_block_start\ndata: {{\"type\":\"content_block_start\",\"index\":0,\"content_block\":{{\"type\":\"text\",\"text\":\"\"}}}}\n\nevent: content_block_delta\ndata: {{\"type\":\"content_block_delta\",\"index\":0,\"delta\":{{\"type\":\"text_delta\",\"text\":\"safe\"}}}}\n\nevent: content_block_stop\ndata: {{\"type\":\"content_block_stop\",\"index\":0}}\n\nevent: content_block_start\ndata: {{\"type\":\"content_block_start\",\"index\":1,\"content_block\":{{\"type\":\"tool_use\",\"id\":\"tool_1\",\"name\":\"Bash\",\"input\":{{}}}}}}\n\nevent: content_block_delta\ndata: {delta}\n\nevent: message_stop\ndata: {{\"type\":\"message_stop\"}}\n\n"
        );
        let output = create_logged_passthrough_stream(
            futures::stream::iter(vec![Ok::<Bytes, std::io::Error>(Bytes::from(source))]),
            "test",
            None,
            StreamingTimeoutConfig {
                first_byte_timeout: 0,
                idle_timeout: 0,
            },
            None,
            SseTerminalPolicy::ClaudeDesktopAnthropic,
        )
        .collect::<Vec<_>>()
        .await;
        let text = output
            .into_iter()
            .map(Result::unwrap)
            .fold(Vec::new(), |mut all, bytes| {
                all.extend_from_slice(&bytes);
                all
            });
        let text = String::from_utf8(text).unwrap();
        assert!(text.contains("safe"), "{text}");
        assert!(!text.contains("CC Switch"), "{text}");
        assert!(!text.contains("安全取消"), "{text}");
        assert!(!text.contains("自动续试"), "{text}");
        assert!(
            !text.contains(&"x".repeat(1024)),
            "oversized tool payload leaked"
        );
        assert!(text.contains("event: message_stop"), "{text}");
        assert!(!text.contains("event: error"), "{text}");
    }

    #[tokio::test]
    async fn claude_desktop_closed_server_tools_allow_text_eof_repair() {
        let source = futures::stream::iter(vec![Ok::<Bytes, std::io::Error>(Bytes::from_static(
            b"event: message_start\ndata: {\"type\":\"message_start\"}\n\nevent: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"server_tool_use\",\"id\":\"srv_1\",\"name\":\"web_search\",\"input\":{}}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"query\\\":\\\"rust\\\"}\"}}\n\nevent: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\nevent: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"web_search_tool_result\",\"tool_use_id\":\"srv_1\",\"content\":[]}}\n\nevent: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":1}\n\nevent: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":2,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":2,\"delta\":{\"type\":\"text_delta\",\"text\":\"partial web answer\"}}\n\n",
        ))]);
        let output = create_logged_passthrough_stream(
            source,
            "test",
            None,
            StreamingTimeoutConfig {
                first_byte_timeout: 0,
                idle_timeout: 0,
            },
            None,
            SseTerminalPolicy::ClaudeDesktopAnthropic,
        )
        .collect::<Vec<_>>()
        .await;
        let text = output
            .into_iter()
            .map(Result::unwrap)
            .fold(Vec::new(), |mut all, bytes| {
                all.extend_from_slice(&bytes);
                all
            });
        let text = String::from_utf8(text).unwrap();
        assert!(text.contains("partial web answer"), "{text}");
        assert!(text.contains("event: message_stop"), "{text}");
        assert!(!text.contains("event: error"), "{text}");
    }

    #[tokio::test]
    async fn claude_desktop_caps_unframed_sse_event() {
        let oversized = "z".repeat(MAX_UNFRAMED_ANTHROPIC_SSE_BYTES + 1);
        let incomplete = format!(
            "event: content_block_delta\ndata: {{\"type\":\"content_block_delta\",\"index\":0,\"delta\":{{\"type\":\"text_delta\",\"text\":\"{oversized}"
        );
        let source = futures::stream::iter(vec![
            Ok::<Bytes, std::io::Error>(Bytes::from_static(
                b"event: message_start\ndata: {\"type\":\"message_start\"}\n\nevent: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"safe prefix\"}}\n\n",
            )),
            Ok(Bytes::from(incomplete)),
        ]);
        let output = create_logged_passthrough_stream(
            source,
            "test",
            None,
            StreamingTimeoutConfig {
                first_byte_timeout: 0,
                idle_timeout: 0,
            },
            None,
            SseTerminalPolicy::ClaudeDesktopAnthropic,
        )
        .collect::<Vec<_>>()
        .await;
        let text = output
            .into_iter()
            .map(Result::unwrap)
            .fold(Vec::new(), |mut all, bytes| {
                all.extend_from_slice(&bytes);
                all
            });
        let text = String::from_utf8(text).unwrap();
        assert!(text.contains("safe prefix"), "{text}");
        assert!(!text.contains(&"z".repeat(1024)), "unframed payload leaked");
        assert!(text.contains("event: message_stop"), "{text}");
        assert!(!text.contains("event: error"), "{text}");
    }

    #[tokio::test]
    async fn anthropic_text_start_without_delta_is_not_repaired_as_success() {
        let source = futures::stream::iter(vec![Ok::<Bytes, std::io::Error>(Bytes::from_static(
            b"event: message_start\ndata: {\"type\":\"message_start\"}\n\nevent: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
        ))]);
        let output = create_logged_passthrough_stream(
            source,
            "test",
            None,
            StreamingTimeoutConfig {
                first_byte_timeout: 0,
                idle_timeout: 0,
            },
            None,
            SseTerminalPolicy::AnthropicMessages,
        )
        .collect::<Vec<_>>()
        .await;
        let text = output
            .into_iter()
            .map(Result::unwrap)
            .fold(Vec::new(), |mut all, bytes| {
                all.extend_from_slice(&bytes);
                all
            });
        let text = String::from_utf8(text).unwrap();
        assert!(text.contains("event: error"), "{text}");
        assert!(!text.contains("\"stop_reason\":\"max_tokens\""), "{text}");
    }

    #[tokio::test]
    async fn anthropic_text_io_error_preserves_partial_text_as_completed_turn() {
        let source = futures::stream::iter(vec![
            Ok::<Bytes, std::io::Error>(Bytes::from_static(
                b"event: message_start\ndata: {\"type\":\"message_start\"}\n\nevent: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"partial\"}}\n\n",
            )),
            Err(std::io::Error::other("connection reset")),
        ]);
        let output = create_logged_passthrough_stream(
            source,
            "test",
            None,
            StreamingTimeoutConfig {
                first_byte_timeout: 0,
                idle_timeout: 0,
            },
            None,
            SseTerminalPolicy::ClaudeDesktopAnthropic,
        )
        .collect::<Vec<_>>()
        .await;
        let text = output
            .into_iter()
            .map(Result::unwrap)
            .fold(Vec::new(), |mut all, bytes| {
                all.extend_from_slice(&bytes);
                all
            });
        let text = String::from_utf8(text).unwrap();
        assert!(text.contains("event: content_block_stop"), "{text}");
        assert!(text.contains("\"stop_reason\":\"max_tokens\""), "{text}");
        assert!(text.contains("event: message_stop"), "{text}");
        assert!(!text.contains("event: error"), "{text}");
    }

    #[test]
    fn test_strip_hop_by_hop_response_headers_removes_standard_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::CONNECTION,
            axum::http::HeaderValue::from_static("keep-alive"),
        );
        headers.insert(
            axum::http::header::HeaderName::from_static("keep-alive"),
            axum::http::HeaderValue::from_static("timeout=5"),
        );
        headers.insert(
            axum::http::header::TRANSFER_ENCODING,
            axum::http::HeaderValue::from_static("chunked"),
        );
        headers.insert(
            axum::http::header::HeaderName::from_static("proxy-connection"),
            axum::http::HeaderValue::from_static("keep-alive"),
        );
        headers.insert(
            axum::http::header::CONTENT_TYPE,
            axum::http::HeaderValue::from_static("application/json"),
        );
        headers.insert(
            axum::http::header::CONTENT_LENGTH,
            axum::http::HeaderValue::from_static("12"),
        );

        strip_hop_by_hop_response_headers(&mut headers);

        assert!(!headers.contains_key(axum::http::header::CONNECTION));
        assert!(!headers.contains_key("keep-alive"));
        assert!(!headers.contains_key(axum::http::header::TRANSFER_ENCODING));
        assert!(!headers.contains_key("proxy-connection"));
        assert_eq!(
            headers.get(axum::http::header::CONTENT_TYPE),
            Some(&axum::http::HeaderValue::from_static("application/json"))
        );
        assert_eq!(
            headers.get(axum::http::header::CONTENT_LENGTH),
            Some(&axum::http::HeaderValue::from_static("12"))
        );
    }

    #[test]
    fn test_strip_hop_by_hop_response_headers_removes_connection_listed_extensions() {
        let mut headers = HeaderMap::new();
        headers.append(
            axum::http::header::CONNECTION,
            axum::http::HeaderValue::from_static("x-trace-hop, x-debug-hop"),
        );
        headers.append(
            axum::http::header::CONNECTION,
            axum::http::HeaderValue::from_static("upgrade"),
        );
        headers.insert(
            axum::http::header::HeaderName::from_static("x-trace-hop"),
            axum::http::HeaderValue::from_static("trace"),
        );
        headers.insert(
            axum::http::header::HeaderName::from_static("x-debug-hop"),
            axum::http::HeaderValue::from_static("debug"),
        );
        headers.insert(
            axum::http::header::UPGRADE,
            axum::http::HeaderValue::from_static("websocket"),
        );
        headers.insert(
            axum::http::header::CONTENT_TYPE,
            axum::http::HeaderValue::from_static("text/event-stream"),
        );

        strip_hop_by_hop_response_headers(&mut headers);

        assert!(!headers.contains_key(axum::http::header::CONNECTION));
        assert!(!headers.contains_key("x-trace-hop"));
        assert!(!headers.contains_key("x-debug-hop"));
        assert!(!headers.contains_key(axum::http::header::UPGRADE));
        assert_eq!(
            headers.get(axum::http::header::CONTENT_TYPE),
            Some(&axum::http::HeaderValue::from_static("text/event-stream"))
        );
    }

    fn build_state(db: Arc<Database>) -> ProxyState {
        ProxyState {
            db: db.clone(),
            config: Arc::new(RwLock::new(ProxyConfig::default())),
            status: Arc::new(RwLock::new(ProxyStatus::default())),
            start_time: Arc::new(RwLock::new(None)),
            current_providers: Arc::new(RwLock::new(HashMap::new())),
            provider_router: Arc::new(ProviderRouter::new(db.clone())),
            gemini_shadow: Arc::new(GeminiShadowStore::default()),
            codex_chat_history: Arc::new(CodexChatHistoryStore::default()),
            app_handle: None,
            failover_manager: Arc::new(FailoverSwitchManager::new(db)),
        }
    }

    fn seed_pricing(db: &Database) -> Result<(), AppError> {
        let conn = crate::database::lock_conn!(db.conn);
        conn.execute(
            "INSERT OR REPLACE INTO model_pricing (model_id, display_name, input_cost_per_million, output_cost_per_million)
             VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params!["resp-model", "Resp Model", "1.0", "0"],
        )
        .map_err(|e| AppError::Database(e.to_string()))?;
        conn.execute(
            "INSERT OR REPLACE INTO model_pricing (model_id, display_name, input_cost_per_million, output_cost_per_million)
             VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params!["req-model", "Req Model", "2.0", "0"],
        )
        .map_err(|e| AppError::Database(e.to_string()))?;
        Ok(())
    }

    fn insert_provider(
        db: &Database,
        id: &str,
        app_type: &str,
        meta: ProviderMeta,
    ) -> Result<(), AppError> {
        let meta_json =
            serde_json::to_string(&meta).map_err(|e| AppError::Database(e.to_string()))?;
        let conn = crate::database::lock_conn!(db.conn);
        conn.execute(
            "INSERT INTO providers (id, app_type, name, settings_config, meta)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![id, app_type, "Test Provider", "{}", meta_json],
        )
        .map_err(|e| AppError::Database(e.to_string()))?;
        Ok(())
    }

    #[tokio::test]
    async fn test_log_usage_uses_provider_override_config() -> Result<(), AppError> {
        let db = Arc::new(Database::memory()?);
        let app_type = "claude";

        db.set_default_cost_multiplier(app_type, "1.5").await?;
        db.set_pricing_model_source(app_type, "response").await?;
        seed_pricing(&db)?;

        let meta = ProviderMeta {
            cost_multiplier: Some("2".to_string()),
            pricing_model_source: Some("request".to_string()),
            ..ProviderMeta::default()
        };
        insert_provider(&db, "provider-1", app_type, meta)?;

        let state = build_state(db.clone());
        let usage = TokenUsage {
            input_tokens: 1_000_000,
            output_tokens: 0,
            cache_read_tokens: 0,
            cache_creation_tokens: 0,
            model: None,
            message_id: None,
        };

        log_usage_internal(
            &state,
            "provider-1",
            app_type,
            "resp-model",
            "req-model",
            "req-model",
            usage,
            10,
            None,
            false,
            200,
            None,
            None,
        )
        .await;

        let conn = crate::database::lock_conn!(db.conn);
        let (model, request_model, total_cost, cost_multiplier): (String, String, String, String) =
            conn.query_row(
                "SELECT model, request_model, total_cost_usd, cost_multiplier
                 FROM proxy_request_logs WHERE provider_id = ?1",
                ["provider-1"],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .map_err(|e| AppError::Database(e.to_string()))?;

        assert_eq!(model, "resp-model");
        assert_eq!(request_model, "req-model");
        assert_eq!(
            Decimal::from_str(&cost_multiplier).unwrap(),
            Decimal::from_str("2").unwrap()
        );
        assert_eq!(
            Decimal::from_str(&total_cost).unwrap(),
            Decimal::from_str("4").unwrap()
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_request_pricing_mode_anchors_to_outbound_model() -> Result<(), AppError> {
        let db = Arc::new(Database::memory()?);
        let app_type = "claude";

        db.set_pricing_model_source(app_type, "request").await?;
        seed_pricing(&db)?;
        {
            let conn = crate::database::lock_conn!(db.conn);
            conn.execute(
                "INSERT OR REPLACE INTO model_pricing (model_id, display_name, input_cost_per_million, output_cost_per_million)
                 VALUES ('outbound-model', 'Outbound Model', '4.0', '0')",
                [],
            )
            .map_err(|e| AppError::Database(e.to_string()))?;
        }

        insert_provider(&db, "provider-3", app_type, ProviderMeta::default())?;

        let state = build_state(db.clone());
        let usage = TokenUsage {
            input_tokens: 1_000_000,
            output_tokens: 0,
            cache_read_tokens: 0,
            cache_creation_tokens: 0,
            model: None,
            message_id: None,
        };

        // 路由接管场景：客户端请求 req-model（$2/M），代理实际发出 outbound-model
        // （$4/M），上游回显 resp-model。「按请求计价」必须锚定实际发出的模型。
        log_usage_internal(
            &state,
            "provider-3",
            app_type,
            "resp-model",
            "req-model",
            "outbound-model",
            usage,
            10,
            None,
            false,
            200,
            None,
            None,
        )
        .await;

        let conn = crate::database::lock_conn!(db.conn);
        let (model, request_model, total_cost): (String, String, String) = conn
            .query_row(
                "SELECT model, request_model, total_cost_usd
                 FROM proxy_request_logs WHERE provider_id = ?1",
                ["provider-3"],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(|e| AppError::Database(e.to_string()))?;

        // model / request_model 列不受计价锚点影响
        assert_eq!(model, "resp-model");
        assert_eq!(request_model, "req-model");
        // 按 outbound-model（$4/M）计价，而不是 req-model（$2/M）或 resp-model（$1/M）
        assert_eq!(
            Decimal::from_str(&total_cost).unwrap(),
            Decimal::from_str("4").unwrap()
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_claude_desktop_inherits_claude_global_defaults() -> Result<(), AppError> {
        use crate::proxy::usage::logger::UsageLogger;

        let db = Arc::new(Database::memory()?);

        // 全局计费配置只有 claude/codex/gemini 三行；claude-desktop 的
        // 全局默认必须继承 claude，而不是静默落回工厂默认（1 / response）
        db.set_default_cost_multiplier("claude", "1.5").await?;
        db.set_pricing_model_source("claude", "request").await?;

        let logger = UsageLogger::new(&db);
        let (multiplier, source) = logger
            .resolve_pricing_config("nonexistent-provider", "claude-desktop")
            .await;

        assert_eq!(multiplier, Decimal::from_str("1.5").unwrap());
        assert_eq!(source, "request");
        Ok(())
    }

    #[tokio::test]
    async fn test_log_usage_falls_back_to_global_defaults() -> Result<(), AppError> {
        let db = Arc::new(Database::memory()?);
        let app_type = "claude";

        db.set_default_cost_multiplier(app_type, "1.5").await?;
        db.set_pricing_model_source(app_type, "response").await?;
        seed_pricing(&db)?;

        let meta = ProviderMeta::default();
        insert_provider(&db, "provider-2", app_type, meta)?;

        let state = build_state(db.clone());
        let usage = TokenUsage {
            input_tokens: 1_000_000,
            output_tokens: 0,
            cache_read_tokens: 0,
            cache_creation_tokens: 0,
            model: None,
            message_id: None,
        };

        log_usage_internal(
            &state,
            "provider-2",
            app_type,
            "resp-model",
            "req-model",
            "req-model",
            usage,
            10,
            None,
            false,
            200,
            None,
            None,
        )
        .await;

        let conn = crate::database::lock_conn!(db.conn);
        let (total_cost, cost_multiplier): (String, String) = conn
            .query_row(
                "SELECT total_cost_usd, cost_multiplier
                 FROM proxy_request_logs WHERE provider_id = ?1",
                ["provider-2"],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(|e| AppError::Database(e.to_string()))?;

        assert_eq!(
            Decimal::from_str(&cost_multiplier).unwrap(),
            Decimal::from_str("1.5").unwrap()
        );
        assert_eq!(
            Decimal::from_str(&total_cost).unwrap(),
            Decimal::from_str("1.5").unwrap()
        );
        Ok(())
    }
}
