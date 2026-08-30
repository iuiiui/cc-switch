//! 供应商路由器模块
//!
//! 负责选择和管理代理目标供应商，实现智能故障转移

use crate::app_config::AppType;
use crate::database::Database;
use crate::error::AppError;
use crate::provider::Provider;
use crate::proxy::circuit_breaker::{
    AllowResult, CircuitBreaker, CircuitBreakerConfig, RecordDisposition,
};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Codex Official requests carry the selected account's native Authorization
/// header. Reusing that request against another account card would cross the
/// account boundary, so these cards must never participate in provider retry.
pub(crate) fn provider_supports_failover(app_type: &str, provider: &Provider) -> bool {
    if app_type == AppType::ClaudeDesktop.as_str() {
        return !crate::claude_desktop_config::is_official_provider(provider)
            && matches!(
                crate::claude_desktop_config::provider_mode(provider),
                crate::provider::ClaudeDesktopMode::Proxy
            )
            && crate::claude_desktop_config::validate_proxy_provider(provider).is_ok();
    }

    app_type != AppType::Codex.as_str()
        || !crate::proxy::providers::is_codex_official_provider(provider)
}

/// 供应商路由器
pub struct ProviderRouter {
    /// 数据库连接
    db: Arc<Database>,
    /// 熔断器管理器 - key 格式: "app_type:provider_id"
    circuit_breakers: Arc<RwLock<HashMap<String, Arc<CircuitBreaker>>>>,
    /// 429/配额耗尽后的短期冷却，进程重启后自然清空。
    rate_limit_cooldowns: Arc<RwLock<HashMap<String, Instant>>>,
}

impl ProviderRouter {
    /// 创建新的供应商路由器
    pub fn new(db: Arc<Database>) -> Self {
        Self {
            db,
            circuit_breakers: Arc::new(RwLock::new(HashMap::new())),
            rate_limit_cooldowns: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 选择可用的供应商（支持故障转移）
    ///
    /// 返回按优先级排序的可用供应商列表：
    /// - 故障转移关闭时：仅返回当前供应商
    /// - 故障转移开启时：仅使用故障转移队列，按队列顺序依次尝试（P1 → P2 → ...）
    pub async fn select_providers(&self, app_type: &str) -> Result<Vec<Provider>, AppError> {
        let mut result = Vec::new();
        let mut total_providers = 0usize;
        let mut circuit_open_count = 0usize;
        let current_id = AppType::from_str(app_type)
            .ok()
            .and_then(|app_enum| {
                crate::settings::get_effective_current_provider(&self.db, &app_enum)
                    .ok()
                    .flatten()
            })
            .or_else(|| self.db.get_current_provider(app_type).ok().flatten());
        let current_provider = current_id
            .as_deref()
            .map(|id| self.db.get_provider_by_id(id, app_type))
            .transpose()?
            .flatten();

        // 检查该应用的自动故障转移开关是否开启（从 proxy_config 表读取）
        let auto_failover_enabled = match self.db.get_proxy_config_for_app(app_type).await {
            Ok(config) => config.auto_failover_enabled,
            Err(e) => {
                log::error!("[{app_type}] 读取 proxy_config 失败: {e}，默认禁用故障转移");
                false
            }
        };
        let policy = self.db.get_failover_policy(app_type).unwrap_or_default();

        if auto_failover_enabled
            && current_provider
                .as_ref()
                .is_some_and(|provider| !provider_supports_failover(app_type, provider))
        {
            // A selected Codex Official account is an explicit account choice.
            // Keep it as a single route even if an old failover setting remains
            // enabled; retrying would reuse its inbound token for another card.
            total_providers = 1;
            result.push(current_provider.expect("checked above"));
        } else if auto_failover_enabled {
            // 故障转移开启：仅按队列顺序依次尝试（P1 → P2 → ...）
            let all_providers = self.db.get_all_providers(app_type)?;
            let desktop_waits_for_recovery = app_type == AppType::ClaudeDesktop.as_str();

            // 使用 DAO 返回的排序结果，确保和前端展示一致
            let mut ordered_ids: Vec<String> = self
                .db
                .get_failover_queue(app_type)?
                .into_iter()
                .map(|item| item.provider_id)
                .collect();

            if matches!(
                policy.strategy,
                crate::proxy::types::FailoverStrategy::StickyRotation
            ) {
                if let Some(current_id) = current_id.as_deref() {
                    if let Some(index) = ordered_ids.iter().position(|id| id == current_id) {
                        ordered_ids.rotate_left(index);
                    }
                }
            }

            for provider_id in ordered_ids {
                let Some(provider) = all_providers.get(&provider_id).cloned() else {
                    continue;
                };
                if !provider_supports_failover(app_type, &provider) {
                    continue;
                }
                total_providers += 1;

                if matches!(
                    policy.strategy,
                    crate::proxy::types::FailoverStrategy::StickyRotation
                ) && self.is_provider_rate_limited(&provider.id, app_type).await
                {
                    circuit_open_count += 1;
                    // Claude Desktop 由 forwarder 在当前客户端请求内等待 cooldown；
                    // 这里若先过滤掉全部候选，就会绕过内部轮询并直接返回 503。
                    if !desktop_waits_for_recovery {
                        continue;
                    }
                }

                let circuit_key = format!("{app_type}:{}", provider.id);
                let breaker = self.get_or_create_circuit_breaker(&circuit_key).await;

                if breaker.is_available().await || desktop_waits_for_recovery {
                    result.push(provider);
                } else {
                    circuit_open_count += 1;
                }
            }
        } else {
            // 故障转移关闭：仅使用当前供应商，跳过熔断器检查
            if let Some(current) = current_provider {
                total_providers = 1;
                result.push(current);
            }
        }

        if result.is_empty() {
            if total_providers > 0 && circuit_open_count == total_providers {
                log::warn!("[{app_type}] [FO-004] 所有供应商均已熔断");
                return Err(AppError::AllProvidersCircuitOpen);
            } else {
                log::warn!("[{app_type}] [FO-005] 未配置供应商");
                return Err(AppError::NoProvidersConfigured);
            }
        }

        Ok(result)
    }

    pub async fn mark_provider_rate_limited(
        &self,
        provider_id: &str,
        app_type: &str,
    ) -> Result<Duration, AppError> {
        let policy = self.db.get_failover_policy(app_type)?;
        if !matches!(
            policy.strategy,
            crate::proxy::types::FailoverStrategy::StickyRotation
        ) {
            return Ok(Duration::ZERO);
        }

        let seconds = policy
            .rate_limit_cooldown_seconds
            .min(policy.max_rate_limit_cooldown_seconds)
            .max(1);
        let duration = Duration::from_secs(seconds as u64);
        let key = format!("{app_type}:{provider_id}");
        let now = Instant::now();
        let mut cooldowns = self.rate_limit_cooldowns.write().await;
        if let Some(until) = cooldowns.get(&key).copied().filter(|until| *until > now) {
            return Ok(until.saturating_duration_since(now));
        }
        cooldowns.insert(key, now + duration);
        drop(cooldowns);
        log::warn!("[{app_type}] 供应商 {provider_id} 触发速率限制，冷却 {seconds} 秒后再参与轮转");
        Ok(duration)
    }

    pub async fn is_provider_rate_limited(&self, provider_id: &str, app_type: &str) -> bool {
        let key = format!("{app_type}:{provider_id}");
        let mut cooldowns = self.rate_limit_cooldowns.write().await;
        match cooldowns.get(&key).copied() {
            Some(until) if until > Instant::now() => true,
            Some(_) => {
                cooldowns.remove(&key);
                false
            }
            None => false,
        }
    }

    pub async fn earliest_rate_limit_retry_delay(
        &self,
        providers: &[Provider],
        app_type: &str,
    ) -> Option<Duration> {
        let now = Instant::now();
        let mut cooldowns = self.rate_limit_cooldowns.write().await;
        cooldowns.retain(|_, until| *until > now);
        providers
            .iter()
            .filter_map(|provider| {
                cooldowns
                    .get(&format!("{app_type}:{}", provider.id))
                    .map(|until| until.saturating_duration_since(now))
            })
            .min()
    }

    /// 清除指定应用的运行期熔断器与速率限制冷却。
    /// 健康状态属于当前代理进程，重新启用故障转移时不继承上一轮状态。
    pub async fn reset_app_runtime_state(&self, app_type: &str) {
        let prefix = format!("{app_type}:");
        self.circuit_breakers
            .write()
            .await
            .retain(|key, _| !key.starts_with(&prefix));
        self.rate_limit_cooldowns
            .write()
            .await
            .retain(|key, _| !key.starts_with(&prefix));
    }

    /// 请求执行前获取熔断器“放行许可”
    ///
    /// - Closed：直接放行
    /// - Open：超时到达后切到 HalfOpen 并放行一次探测
    /// - HalfOpen：按限流规则放行探测
    ///
    /// 注意：调用方必须在请求结束后通过 `record_result()` 释放 HalfOpen 名额，
    /// 否则会导致该 Provider 长时间无法进入探测状态。
    #[allow(dead_code)]
    pub async fn allow_provider_request(&self, provider_id: &str, app_type: &str) -> AllowResult {
        let circuit_key = format!("{app_type}:{provider_id}");
        let breaker = self.get_or_create_circuit_breaker(&circuit_key).await;
        breaker.allow_request().await
    }

    pub async fn begin_provider_request(
        &self,
        provider_id: &str,
        app_type: &str,
        bypass_circuit_gate: bool,
    ) -> AllowResult {
        let circuit_key = format!("{app_type}:{provider_id}");
        let breaker = self.get_or_create_circuit_breaker(&circuit_key).await;
        if bypass_circuit_gate {
            breaker.tracking_permit()
        } else {
            breaker.allow_request().await
        }
    }

    /// 记录供应商请求结果
    pub async fn record_result(
        &self,
        provider_id: &str,
        app_type: &str,
        permit: AllowResult,
        success: bool,
        error_msg: Option<String>,
    ) -> Result<(), AppError> {
        // 1. 按应用独立获取熔断器配置
        let failure_threshold = match self.db.get_proxy_config_for_app(app_type).await {
            Ok(app_config) => app_config.circuit_failure_threshold,
            Err(_) => 5, // 默认值
        };

        // 2. 更新熔断器状态
        let circuit_key = format!("{app_type}:{provider_id}");
        let breaker = self.get_or_create_circuit_breaker(&circuit_key).await;

        let disposition = if success {
            breaker.record_success(permit).await
        } else {
            breaker.record_failure(permit).await
        };

        if disposition != RecordDisposition::Counted {
            return Ok(());
        }

        // 3. 更新数据库健康状态（使用配置的阈值）
        self.db
            .update_provider_health_with_threshold(
                provider_id,
                app_type,
                success,
                error_msg.clone(),
                failure_threshold,
            )
            .await?;

        Ok(())
    }

    /// 重置指定供应商的熔断器
    pub async fn reset_provider_breaker(&self, provider_id: &str, app_type: &str) {
        let circuit_key = format!("{app_type}:{provider_id}");
        self.circuit_breakers.write().await.remove(&circuit_key);
        self.rate_limit_cooldowns.write().await.remove(&circuit_key);
    }

    /// 仅释放 HalfOpen permit，不影响健康统计（neutral 接口）
    ///
    /// 用于整流器等场景：请求结果不应计入 Provider 健康度，
    /// 但仍需释放占用的探测名额，避免 HalfOpen 状态卡死
    pub async fn release_permit_neutral(
        &self,
        provider_id: &str,
        app_type: &str,
        permit: AllowResult,
    ) {
        let circuit_key = format!("{app_type}:{provider_id}");
        let breaker = self.get_or_create_circuit_breaker(&circuit_key).await;
        breaker.release_neutral(permit);
    }

    /// 更新所有熔断器的配置（热更新）
    pub async fn update_all_configs(&self, config: CircuitBreakerConfig) {
        let breakers = self.circuit_breakers.read().await;
        for breaker in breakers.values() {
            breaker.update_config(config.clone()).await;
        }
    }

    /// 更新指定应用已创建熔断器的配置（热更新）
    pub async fn update_app_configs(&self, app_type: &str, config: CircuitBreakerConfig) {
        let prefix = format!("{app_type}:");
        let breakers = self.circuit_breakers.read().await;
        for (key, breaker) in breakers.iter() {
            if key.starts_with(&prefix) {
                breaker.update_config(config.clone()).await;
            }
        }
    }

    /// 获取熔断器状态
    #[allow(dead_code)]
    pub async fn get_circuit_breaker_stats(
        &self,
        provider_id: &str,
        app_type: &str,
    ) -> Option<crate::proxy::circuit_breaker::CircuitBreakerStats> {
        let circuit_key = format!("{app_type}:{provider_id}");
        let breakers = self.circuit_breakers.read().await;

        if let Some(breaker) = breakers.get(&circuit_key) {
            Some(breaker.get_stats().await)
        } else {
            None
        }
    }

    /// 获取或创建熔断器
    async fn get_or_create_circuit_breaker(&self, key: &str) -> Arc<CircuitBreaker> {
        // 先尝试读锁获取
        {
            let breakers = self.circuit_breakers.read().await;
            if let Some(breaker) = breakers.get(key) {
                return breaker.clone();
            }
        }

        // 如果不存在，获取写锁创建
        let mut breakers = self.circuit_breakers.write().await;

        // 双重检查，防止竞争条件
        if let Some(breaker) = breakers.get(key) {
            return breaker.clone();
        }

        // 从 key 中提取 app_type (格式: "app_type:provider_id")
        let app_type = key.split(':').next().unwrap_or("claude");

        // 按应用独立读取熔断器配置
        let config = match self.db.get_proxy_config_for_app(app_type).await {
            Ok(app_config) => crate::proxy::circuit_breaker::CircuitBreakerConfig {
                failure_threshold: app_config.circuit_failure_threshold,
                success_threshold: app_config.circuit_success_threshold,
                timeout_seconds: app_config.circuit_timeout_seconds as u64,
                error_rate_threshold: app_config.circuit_error_rate_threshold,
                min_requests: app_config.circuit_min_requests,
            },
            Err(_) => crate::proxy::circuit_breaker::CircuitBreakerConfig::default(),
        };

        let breaker = Arc::new(CircuitBreaker::new(config));
        breakers.insert(key.to_string(), breaker.clone());

        breaker
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::Database;
    use crate::provider::{
        AuthBinding, AuthBindingSource, ClaudeDesktopMode, ClaudeDesktopModelRoute, ProviderMeta,
    };
    use serde_json::json;
    use serial_test::serial;
    use std::env;
    use tempfile::TempDir;

    fn managed_codex_official(id: &str, account_id: &str) -> Provider {
        let mut provider = Provider::with_id(
            id.to_string(),
            "OpenAI Official".to_string(),
            json!({ "auth": {}, "config": "" }),
            None,
        );
        provider.category = Some("official".to_string());
        provider.meta = Some(ProviderMeta {
            provider_type: Some("codex_oauth".to_string()),
            auth_binding: Some(AuthBinding {
                source: AuthBindingSource::ManagedAccount,
                auth_provider: Some("codex_oauth".to_string()),
                account_id: Some(account_id.to_string()),
            }),
            ..Default::default()
        });
        provider
    }

    fn desktop_proxy_provider(id: &str) -> Provider {
        let mut provider = Provider::with_id(
            id.to_string(),
            format!("Desktop {id}"),
            json!({
                "env": {
                    "ANTHROPIC_BASE_URL": "https://api.example.com/anthropic",
                    "ANTHROPIC_AUTH_TOKEN": "test-token"
                }
            }),
            None,
        );
        provider.meta = Some(ProviderMeta {
            claude_desktop_mode: Some(ClaudeDesktopMode::Proxy),
            api_format: Some("anthropic".to_string()),
            claude_desktop_model_routes: std::collections::HashMap::from([(
                "claude-sonnet-5".to_string(),
                ClaudeDesktopModelRoute {
                    model: "upstream-model".to_string(),
                    label_override: None,
                    supports_1m: Some(true),
                },
            )]),
            ..Default::default()
        });
        provider
    }

    struct TempHome {
        #[allow(dead_code)]
        dir: TempDir,
        original_home: Option<String>,
        original_userprofile: Option<String>,
        original_test_home: Option<String>,
    }

    impl TempHome {
        fn new() -> Self {
            let dir = TempDir::new().expect("failed to create temp home");
            let original_home = env::var("HOME").ok();
            let original_userprofile = env::var("USERPROFILE").ok();
            let original_test_home = env::var("CC_SWITCH_TEST_HOME").ok();

            env::set_var("HOME", dir.path());
            env::set_var("USERPROFILE", dir.path());
            env::set_var("CC_SWITCH_TEST_HOME", dir.path());
            crate::settings::reload_settings().expect("reload settings");

            Self {
                dir,
                original_home,
                original_userprofile,
                original_test_home,
            }
        }
    }

    impl Drop for TempHome {
        fn drop(&mut self) {
            match &self.original_home {
                Some(value) => env::set_var("HOME", value),
                None => env::remove_var("HOME"),
            }

            match &self.original_userprofile {
                Some(value) => env::set_var("USERPROFILE", value),
                None => env::remove_var("USERPROFILE"),
            }

            match &self.original_test_home {
                Some(value) => env::set_var("CC_SWITCH_TEST_HOME", value),
                None => env::remove_var("CC_SWITCH_TEST_HOME"),
            }
        }
    }

    #[tokio::test]
    #[serial]
    async fn test_provider_router_creation() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());
        let router = ProviderRouter::new(db);

        let breaker = router.get_or_create_circuit_breaker("claude:test").await;
        assert!(breaker.allow_request().await.allowed);
    }

    #[tokio::test]
    #[serial]
    async fn test_failover_disabled_uses_current_provider() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());

        let provider_a =
            Provider::with_id("a".to_string(), "Provider A".to_string(), json!({}), None);
        let provider_b =
            Provider::with_id("b".to_string(), "Provider B".to_string(), json!({}), None);

        db.save_provider("claude", &provider_a).unwrap();
        db.save_provider("claude", &provider_b).unwrap();
        db.set_current_provider("claude", "a").unwrap();
        db.add_to_failover_queue("claude", "b").unwrap();

        let router = ProviderRouter::new(db.clone());
        let providers = router.select_providers("claude").await.unwrap();

        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0].id, "a");
    }

    #[tokio::test]
    #[serial]
    async fn test_failover_enabled_uses_queue_order_ignoring_current() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());

        // 设置 sort_index 来控制顺序：b=1, a=2
        let mut provider_a =
            Provider::with_id("a".to_string(), "Provider A".to_string(), json!({}), None);
        provider_a.sort_index = Some(2);
        let mut provider_b =
            Provider::with_id("b".to_string(), "Provider B".to_string(), json!({}), None);
        provider_b.sort_index = Some(1);

        db.save_provider("claude", &provider_a).unwrap();
        db.save_provider("claude", &provider_b).unwrap();
        db.set_current_provider("claude", "a").unwrap();

        db.add_to_failover_queue("claude", "b").unwrap();
        db.add_to_failover_queue("claude", "a").unwrap();

        // 启用自动故障转移（使用新的 proxy_config API）
        let mut config = db.get_proxy_config_for_app("claude").await.unwrap();
        config.auto_failover_enabled = true;
        db.update_proxy_config_for_app(config).await.unwrap();

        let router = ProviderRouter::new(db.clone());
        let providers = router.select_providers("claude").await.unwrap();

        assert_eq!(providers.len(), 2);
        // 故障转移开启时：仅按队列顺序选择（忽略当前供应商）
        assert_eq!(providers[0].id, "b");
        assert_eq!(providers[1].id, "a");
    }

    #[tokio::test]
    #[serial]
    async fn claude_desktop_keeps_rate_limited_queue_for_forwarder_waiting() {
        use crate::proxy::types::{FailoverPolicy, FailoverStrategy};

        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());
        for id in ["desktop-a", "desktop-b"] {
            let provider = desktop_proxy_provider(id);
            db.save_provider("claude-desktop", &provider).unwrap();
            db.add_to_failover_queue("claude-desktop", id).unwrap();
        }
        db.set_current_provider("claude-desktop", "desktop-a")
            .unwrap();
        crate::settings::set_current_provider(&AppType::ClaudeDesktop, Some("desktop-a")).unwrap();
        let mut config = db.get_proxy_config_for_app("claude-desktop").await.unwrap();
        config.auto_failover_enabled = true;
        config.circuit_failure_threshold = 1;
        db.update_proxy_config_for_app(config).await.unwrap();
        db.set_failover_policy(
            "claude-desktop",
            &FailoverPolicy {
                strategy: FailoverStrategy::StickyRotation,
                rate_limit_cooldown_seconds: 30,
                max_rate_limit_cooldown_seconds: 300,
            },
        )
        .unwrap();

        let router = ProviderRouter::new(db);
        router
            .mark_provider_rate_limited("desktop-a", "claude-desktop")
            .await
            .unwrap();
        router
            .mark_provider_rate_limited("desktop-b", "claude-desktop")
            .await
            .unwrap();

        let providers = router
            .select_providers("claude-desktop")
            .await
            .expect("Desktop forwarder must receive cooldown candidates");
        assert_eq!(
            providers
                .iter()
                .map(|provider| provider.id.as_str())
                .collect::<Vec<_>>(),
            vec!["desktop-a", "desktop-b"]
        );
        assert!(providers
            .iter()
            .all(|provider| provider_supports_failover("claude-desktop", provider)));

        router.reset_app_runtime_state("claude-desktop").await;
        let permit = router
            .begin_provider_request("desktop-a", "claude-desktop", false)
            .await;
        router
            .record_result(
                "desktop-a",
                "claude-desktop",
                permit,
                false,
                Some("upstream unavailable".to_string()),
            )
            .await
            .unwrap();
        let providers = router
            .select_providers("claude-desktop")
            .await
            .expect("Desktop forwarder must also receive circuit-open candidates");
        assert!(providers.iter().any(|provider| provider.id == "desktop-a"));
    }

    #[tokio::test]
    #[serial]
    async fn sticky_rotation_starts_from_current_and_skips_rate_limited_provider() {
        use crate::proxy::types::{FailoverPolicy, FailoverStrategy};

        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());
        for (id, sort_index) in [("a", 1), ("b", 2), ("c", 3)] {
            let mut provider =
                Provider::with_id(id.to_string(), format!("Provider {id}"), json!({}), None);
            provider.sort_index = Some(sort_index);
            db.save_provider("claude", &provider).unwrap();
            db.add_to_failover_queue("claude", id).unwrap();
        }
        db.set_current_provider("claude", "b").unwrap();
        crate::settings::set_current_provider(&AppType::Claude, Some("b")).unwrap();

        let mut config = db.get_proxy_config_for_app("claude").await.unwrap();
        config.auto_failover_enabled = true;
        db.update_proxy_config_for_app(config).await.unwrap();
        db.set_failover_policy(
            "claude",
            &FailoverPolicy {
                strategy: FailoverStrategy::StickyRotation,
                rate_limit_cooldown_seconds: 60,
                max_rate_limit_cooldown_seconds: 3600,
            },
        )
        .unwrap();

        let router = ProviderRouter::new(db);
        let providers = router.select_providers("claude").await.unwrap();
        assert_eq!(
            providers
                .iter()
                .map(|provider| provider.id.as_str())
                .collect::<Vec<_>>(),
            vec!["b", "c", "a"]
        );

        router
            .mark_provider_rate_limited("b", "claude")
            .await
            .unwrap();
        let providers = router.select_providers("claude").await.unwrap();
        assert_eq!(
            providers
                .iter()
                .map(|provider| provider.id.as_str())
                .collect::<Vec<_>>(),
            vec!["c", "a"]
        );

        router.reset_app_runtime_state("claude").await;
        let providers = router.select_providers("claude").await.unwrap();
        assert_eq!(
            providers
                .iter()
                .map(|provider| provider.id.as_str())
                .collect::<Vec<_>>(),
            vec!["b", "c", "a"]
        );
    }

    #[tokio::test]
    #[serial]
    async fn concurrent_failure_burst_counts_once() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());
        let provider = Provider::with_id("burst".to_string(), "Burst".to_string(), json!({}), None);
        db.save_provider("claude-desktop", &provider).unwrap();

        let mut config = db.get_proxy_config_for_app("claude-desktop").await.unwrap();
        config.circuit_failure_threshold = 3;
        db.update_proxy_config_for_app(config).await.unwrap();

        let router = ProviderRouter::new(db.clone());
        let permits = futures::future::join_all(
            (0..20).map(|_| router.begin_provider_request("burst", "claude-desktop", false)),
        )
        .await;
        for permit in permits {
            router
                .record_result(
                    "burst",
                    "claude-desktop",
                    permit,
                    false,
                    Some("HTTP 429".to_string()),
                )
                .await
                .unwrap();
        }
        let first_batch = db
            .get_provider_health("burst", "claude-desktop")
            .await
            .unwrap();
        assert_eq!(first_batch.consecutive_failures, 1);
        assert!(first_batch.is_healthy);

        let next_permit = router
            .begin_provider_request("burst", "claude-desktop", false)
            .await;
        router
            .record_result(
                "burst",
                "claude-desktop",
                next_permit,
                false,
                Some("HTTP 429".to_string()),
            )
            .await
            .unwrap();
        let next_batch = db
            .get_provider_health("burst", "claude-desktop")
            .await
            .unwrap();
        assert_eq!(next_batch.consecutive_failures, 2);
        assert!(next_batch.is_healthy);
    }

    #[tokio::test]
    #[serial]
    async fn stale_permit_after_runtime_reset_does_not_pollute_health() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());
        let provider = Provider::with_id("stale".to_string(), "Stale".to_string(), json!({}), None);
        db.save_provider("claude-desktop", &provider).unwrap();

        let router = ProviderRouter::new(db.clone());
        let old_permit = router
            .begin_provider_request("stale", "claude-desktop", false)
            .await;
        router
            .reset_provider_breaker("stale", "claude-desktop")
            .await;
        db.reset_provider_health("stale", "claude-desktop")
            .await
            .unwrap();

        router
            .record_result(
                "stale",
                "claude-desktop",
                old_permit,
                false,
                Some("late HTTP 429".to_string()),
            )
            .await
            .unwrap();
        let health = db
            .get_provider_health("stale", "claude-desktop")
            .await
            .unwrap();
        assert_eq!(health.consecutive_failures, 0);
        assert!(health.is_healthy);
    }

    #[tokio::test]
    #[serial]
    async fn test_failover_enabled_uses_queue_only_even_if_current_not_in_queue() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());

        let provider_a =
            Provider::with_id("a".to_string(), "Provider A".to_string(), json!({}), None);
        let mut provider_b =
            Provider::with_id("b".to_string(), "Provider B".to_string(), json!({}), None);
        provider_b.sort_index = Some(1);

        db.save_provider("claude", &provider_a).unwrap();
        db.save_provider("claude", &provider_b).unwrap();
        db.set_current_provider("claude", "a").unwrap();

        // 只把 b 加入故障转移队列（模拟“当前供应商不在队列里”的常见配置）
        db.add_to_failover_queue("claude", "b").unwrap();

        let mut config = db.get_proxy_config_for_app("claude").await.unwrap();
        config.auto_failover_enabled = true;
        db.update_proxy_config_for_app(config).await.unwrap();

        let router = ProviderRouter::new(db.clone());
        let providers = router.select_providers("claude").await.unwrap();

        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0].id, "b");
    }

    #[tokio::test]
    #[serial]
    async fn codex_official_current_stays_single_route_when_failover_is_stale() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());
        let official = managed_codex_official("official-a", "account-a");
        let fallback = Provider::with_id(
            "fallback".to_string(),
            "Fallback".to_string(),
            json!({}),
            None,
        );
        db.save_provider("codex", &official).unwrap();
        db.save_provider("codex", &fallback).unwrap();
        db.set_current_provider("codex", &official.id).unwrap();
        db.add_to_failover_queue("codex", &fallback.id).unwrap();

        let mut config = db.get_proxy_config_for_app("codex").await.unwrap();
        config.auto_failover_enabled = true;
        db.update_proxy_config_for_app(config).await.unwrap();

        let providers = ProviderRouter::new(db)
            .select_providers("codex")
            .await
            .unwrap();
        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0].id, official.id);
    }

    #[tokio::test]
    #[serial]
    async fn stale_codex_official_queue_entries_are_not_retry_targets() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());
        let current = Provider::with_id(
            "third-party".to_string(),
            "Third Party".to_string(),
            json!({}),
            None,
        );
        let official = managed_codex_official("official-a", "account-a");
        let fallback = Provider::with_id(
            "fallback".to_string(),
            "Fallback".to_string(),
            json!({}),
            None,
        );
        db.save_provider("codex", &current).unwrap();
        db.save_provider("codex", &official).unwrap();
        db.save_provider("codex", &fallback).unwrap();
        db.set_current_provider("codex", &current.id).unwrap();
        db.add_to_failover_queue("codex", &official.id).unwrap();
        db.add_to_failover_queue("codex", &fallback.id).unwrap();

        let mut config = db.get_proxy_config_for_app("codex").await.unwrap();
        config.auto_failover_enabled = true;
        db.update_proxy_config_for_app(config).await.unwrap();

        let providers = ProviderRouter::new(db)
            .select_providers("codex")
            .await
            .unwrap();
        assert_eq!(
            providers
                .iter()
                .map(|provider| provider.id.as_str())
                .collect::<Vec<_>>(),
            vec!["fallback"]
        );
    }

    #[tokio::test]
    #[serial]
    async fn test_select_providers_does_not_consume_half_open_permit() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());

        db.update_circuit_breaker_config(&CircuitBreakerConfig {
            failure_threshold: 1,
            timeout_seconds: 0,
            ..Default::default()
        })
        .await
        .unwrap();

        let provider_a =
            Provider::with_id("a".to_string(), "Provider A".to_string(), json!({}), None);
        let provider_b =
            Provider::with_id("b".to_string(), "Provider B".to_string(), json!({}), None);

        db.save_provider("claude", &provider_a).unwrap();
        db.save_provider("claude", &provider_b).unwrap();

        db.add_to_failover_queue("claude", "a").unwrap();
        db.add_to_failover_queue("claude", "b").unwrap();

        // 启用自动故障转移（使用新的 proxy_config API）
        let mut config = db.get_proxy_config_for_app("claude").await.unwrap();
        config.auto_failover_enabled = true;
        db.update_proxy_config_for_app(config).await.unwrap();

        let router = ProviderRouter::new(db.clone());

        let permit = router.begin_provider_request("b", "claude", false).await;
        router
            .record_result("b", "claude", permit, false, Some("fail".to_string()))
            .await
            .unwrap();

        let providers = router.select_providers("claude").await.unwrap();
        assert_eq!(providers.len(), 2);

        assert!(router.allow_provider_request("b", "claude").await.allowed);
    }

    #[tokio::test]
    #[serial]
    async fn test_release_permit_neutral_frees_half_open_slot() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());

        // 配置熔断器：1 次失败即熔断，0 秒超时立即进入 HalfOpen
        db.update_circuit_breaker_config(&CircuitBreakerConfig {
            failure_threshold: 1,
            timeout_seconds: 0,
            ..Default::default()
        })
        .await
        .unwrap();

        let provider_a =
            Provider::with_id("a".to_string(), "Provider A".to_string(), json!({}), None);
        db.save_provider("claude", &provider_a).unwrap();
        db.add_to_failover_queue("claude", "a").unwrap();

        // 启用自动故障转移
        let mut config = db.get_proxy_config_for_app("claude").await.unwrap();
        config.auto_failover_enabled = true;
        db.update_proxy_config_for_app(config).await.unwrap();

        let router = ProviderRouter::new(db.clone());

        // 触发熔断：1 次失败
        let failure_permit = router.begin_provider_request("a", "claude", false).await;
        router
            .record_result(
                "a",
                "claude",
                failure_permit,
                false,
                Some("fail".to_string()),
            )
            .await
            .unwrap();

        // 第一次请求：获取 HalfOpen 探测名额
        let first = router.allow_provider_request("a", "claude").await;
        assert!(first.allowed);
        assert!(first.used_half_open_permit);

        // 第二次请求应被拒绝（名额已被占用）
        let second = router.allow_provider_request("a", "claude").await;
        assert!(!second.allowed);

        // 使用 release_permit_neutral 释放名额（不影响健康统计）
        router.release_permit_neutral("a", "claude", first).await;

        // 第三次请求应被允许（名额已释放）
        let third = router.allow_provider_request("a", "claude").await;
        assert!(third.allowed);
        assert!(third.used_half_open_permit);
    }
}
