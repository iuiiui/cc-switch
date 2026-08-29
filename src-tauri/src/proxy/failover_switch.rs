//! 故障转移切换模块
//!
//! 处理故障转移成功后的供应商切换逻辑，包括：
//! - 去重控制（避免多个请求同时触发）
//! - 托盘菜单更新
//! - 前端事件发射

use crate::database::Database;
use crate::error::AppError;
use std::collections::HashSet;
use std::sync::Arc;
use tauri::{Emitter, Manager};
use tokio::sync::RwLock;

/// 故障转移切换管理器
///
/// 负责处理故障转移成功后的供应商切换，确保 UI 能够直观反映当前使用的供应商。
#[derive(Clone)]
pub struct FailoverSwitchManager {
    /// 正在处理中的切换（key = app_type，同一应用不同目标也必须互斥）
    pending_switches: Arc<RwLock<HashSet<String>>>,
    db: Arc<Database>,
}

impl FailoverSwitchManager {
    pub fn new(db: Arc<Database>) -> Self {
        Self {
            pending_switches: Arc::new(RwLock::new(HashSet::new())),
            db,
        }
    }

    /// 尝试执行故障转移切换
    ///
    /// 如果相同的切换已在进行中，则跳过；否则执行切换逻辑。
    ///
    /// # Returns
    /// - `Ok(true)` - 切换成功执行
    /// - `Ok(false)` - 切换已在进行中，跳过
    /// - `Err(e)` - 切换过程中发生错误
    pub async fn try_switch(
        &self,
        app_handle: Option<&tauri::AppHandle>,
        app_type: &str,
        provider_id: &str,
        provider_name: &str,
    ) -> Result<bool, AppError> {
        self.try_switch_inner(
            app_handle,
            app_type,
            provider_id,
            provider_name,
            SwitchExpectation::Unconditional,
        )
        .await
    }

    pub async fn try_switch_if_current(
        &self,
        app_handle: Option<&tauri::AppHandle>,
        app_type: &str,
        expected_current: Option<&str>,
        provider_id: &str,
        provider_name: &str,
    ) -> Result<bool, AppError> {
        self.try_switch_inner(
            app_handle,
            app_type,
            provider_id,
            provider_name,
            SwitchExpectation::IfCurrent(expected_current),
        )
        .await
    }

    async fn try_switch_inner(
        &self,
        app_handle: Option<&tauri::AppHandle>,
        app_type: &str,
        provider_id: &str,
        provider_name: &str,
        expectation: SwitchExpectation<'_>,
    ) -> Result<bool, AppError> {
        let switch_key = app_type.to_string();

        // 去重检查：如果相同切换已在进行中，跳过
        {
            let mut pending = self.pending_switches.write().await;
            if pending.contains(&switch_key) {
                log::debug!("[Failover] 切换已在进行中，跳过: {app_type} -> {provider_id}");
                return Ok(false);
            }
            pending.insert(switch_key.clone());
        }

        // 执行切换（确保最后清理 pending 标记）
        let result = self
            .do_switch(
                app_handle,
                app_type,
                provider_id,
                provider_name,
                expectation,
            )
            .await;

        // 清理 pending 标记
        {
            let mut pending = self.pending_switches.write().await;
            pending.remove(&switch_key);
        }

        result
    }

    async fn do_switch(
        &self,
        app_handle: Option<&tauri::AppHandle>,
        app_type: &str,
        provider_id: &str,
        provider_name: &str,
        expectation: SwitchExpectation<'_>,
    ) -> Result<bool, AppError> {
        // 检查该应用是否已被代理接管（enabled=true）
        // 只有被接管的应用才允许执行故障转移切换
        let app_enabled = match self.db.get_proxy_config_for_app(app_type).await {
            Ok(config) => config.enabled,
            Err(e) => {
                log::warn!("[FO-002] 无法读取 {app_type} 配置: {e}，跳过切换");
                return Ok(false);
            }
        };

        let is_claude_desktop = app_type == crate::app_config::AppType::ClaudeDesktop.as_str();
        if !app_enabled && !is_claude_desktop {
            log::debug!("[Failover] {app_type} 未启用代理，跳过切换");
            return Ok(false);
        }

        let mut switched = false;

        if let Some(app) = app_handle {
            if let Some(app_state) = app.try_state::<crate::store::AppState>() {
                switched = if is_claude_desktop {
                    match expectation {
                        SwitchExpectation::IfCurrent(expected) => {
                            matches!(
                                crate::services::ProviderService::switch_claude_desktop_if_current(
                                    app_state.inner(),
                                    expected,
                                    provider_id,
                                )?,
                                crate::services::provider::ConditionalSwitchOutcome::Applied
                            )
                        }
                        SwitchExpectation::Unconditional => {
                            let previous = crate::settings::get_effective_current_provider(
                                app_state.db.as_ref(),
                                &crate::app_config::AppType::ClaudeDesktop,
                            )?;
                            if previous.as_deref() == Some(provider_id) {
                                false
                            } else {
                                crate::services::ProviderService::switch(
                                    app_state.inner(),
                                    crate::app_config::AppType::ClaudeDesktop,
                                    provider_id,
                                )?;
                                true
                            }
                        }
                    }
                } else {
                    app_state
                        .proxy_service
                        .hot_switch_provider(app_type, provider_id)
                        .await
                        .map_err(AppError::Message)?
                        .logical_target_changed
                };

                if !switched {
                    return Ok(false);
                }

                log::info!("[FO-001] 切换: {app_type} → {provider_name}");

                if let Ok(new_menu) = crate::tray::create_tray_menu(app, app_state.inner()) {
                    if let Some(tray) = app.tray_by_id(crate::tray::TRAY_ID) {
                        if let Err(e) = tray.set_menu(Some(new_menu)) {
                            log::error!("[Failover] 更新托盘菜单失败: {e}");
                        }
                    }
                }
            }

            // 发射事件到前端
            let event_data = serde_json::json!({
                "appType": app_type,
                "providerId": provider_id,
                "source": "failover"  // 标识来源是故障转移
            });
            if let Err(e) = app.emit("provider-switched", event_data) {
                log::error!("[Failover] 发射事件失败: {e}");
            }
        }

        Ok(switched)
    }
}

#[derive(Clone, Copy)]
enum SwitchExpectation<'a> {
    Unconditional,
    IfCurrent(Option<&'a str>),
}
