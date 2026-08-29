//! 故障转移队列命令
//!
//! 管理代理模式下的故障转移队列（基于 providers 表的 in_failover_queue 字段）

use crate::database::FailoverQueueItem;
use crate::provider::Provider;
use crate::store::AppState;
use std::str::FromStr;
use tauri::Emitter;

fn require_failover_app(app_type: &str) -> Result<(), String> {
    let app = crate::app_config::AppType::from_str(app_type)
        .map_err(|error| format!("无效的应用类型: {error}"))?;
    if !app.supports_local_proxy() && !matches!(app, crate::app_config::AppType::ClaudeDesktop) {
        return Err(format!("{} 不支持故障转移", app.as_str()));
    }
    Ok(())
}

fn require_failover_provider(
    db: &crate::database::Database,
    app_type: &str,
    provider_id: &str,
) -> Result<Provider, String> {
    let provider = db
        .get_provider_by_id(provider_id, app_type)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("供应商不存在: {provider_id}"))?;
    if !crate::proxy::provider_router::provider_supports_failover(app_type, &provider) {
        return Err("Codex Official 账号卡不支持自动故障转移".to_string());
    }
    Ok(provider)
}

#[cfg(test)]
mod tests {
    use super::{require_failover_app, require_failover_provider};
    use crate::database::Database;
    use crate::provider::{AuthBinding, AuthBindingSource, Provider, ProviderMeta};
    use serde_json::json;

    #[test]
    fn failover_rejects_apps_without_a_proxy_data_plane() {
        assert!(require_failover_app("claude").is_ok());
        assert!(require_failover_app("claude-desktop").is_ok());
        assert!(require_failover_app("pi").is_err());
    }

    #[test]
    fn failover_accepts_only_claude_desktop_proxy_providers() {
        use crate::provider::{ClaudeDesktopMode, ClaudeDesktopModelRoute};

        let db = Database::memory().expect("memory db");
        let direct = Provider::with_id(
            "desktop-direct".to_string(),
            "Desktop Direct".to_string(),
            json!({
                "env": {
                    "ANTHROPIC_BASE_URL": "https://example.com",
                    "ANTHROPIC_AUTH_TOKEN": "token"
                }
            }),
            None,
        );
        db.save_provider("claude-desktop", &direct)
            .expect("save direct provider");

        let mut proxy = Provider::with_id(
            "desktop-proxy".to_string(),
            "Desktop Proxy".to_string(),
            json!({
                "env": {
                    "ANTHROPIC_BASE_URL": "https://example.com",
                    "ANTHROPIC_AUTH_TOKEN": "token"
                }
            }),
            None,
        );
        proxy.meta = Some(ProviderMeta {
            claude_desktop_mode: Some(ClaudeDesktopMode::Proxy),
            claude_desktop_model_routes: std::collections::HashMap::from([(
                "claude-sonnet-4-6".to_string(),
                ClaudeDesktopModelRoute {
                    model: "upstream-model".to_string(),
                    label_override: None,
                    supports_1m: None,
                },
            )]),
            ..Default::default()
        });
        db.save_provider("claude-desktop", &proxy)
            .expect("save proxy provider");

        assert!(require_failover_provider(&db, "claude-desktop", &direct.id).is_err());
        assert!(require_failover_provider(&db, "claude-desktop", &proxy.id).is_ok());
    }

    #[test]
    fn failover_rejects_codex_official_account_cards() {
        let db = Database::memory().expect("memory db");
        let mut official = Provider::with_id(
            "official-a".to_string(),
            "OpenAI Official".to_string(),
            json!({ "auth": {}, "config": "" }),
            None,
        );
        official.category = Some("official".to_string());
        official.meta = Some(ProviderMeta {
            auth_binding: Some(AuthBinding {
                source: AuthBindingSource::ManagedAccount,
                auth_provider: Some("codex_oauth".to_string()),
                account_id: Some("account-a".to_string()),
            }),
            ..Default::default()
        });
        db.save_provider("codex", &official).expect("save official");

        assert!(require_failover_provider(&db, "codex", &official.id).is_err());
    }
}

/// 获取故障转移队列
#[tauri::command]
pub async fn get_failover_queue(
    state: tauri::State<'_, AppState>,
    app_type: String,
) -> Result<Vec<FailoverQueueItem>, String> {
    require_failover_app(&app_type)?;
    let queue = state
        .db
        .get_failover_queue(&app_type)
        .map_err(|e| e.to_string())?;
    let providers = state
        .db
        .get_all_providers(&app_type)
        .map_err(|e| e.to_string())?;
    Ok(queue
        .into_iter()
        .filter(|item| {
            providers.get(&item.provider_id).is_some_and(|provider| {
                crate::proxy::provider_router::provider_supports_failover(&app_type, provider)
            })
        })
        .collect())
}

/// 获取可添加到故障转移队列的供应商（不在队列中的）
#[tauri::command]
pub async fn get_available_providers_for_failover(
    state: tauri::State<'_, AppState>,
    app_type: String,
) -> Result<Vec<Provider>, String> {
    require_failover_app(&app_type)?;
    let providers = state
        .db
        .get_available_providers_for_failover(&app_type)
        .map_err(|e| e.to_string())?;
    Ok(providers
        .into_iter()
        .filter(|provider| {
            crate::proxy::provider_router::provider_supports_failover(&app_type, provider)
        })
        .collect())
}

/// 添加供应商到故障转移队列
#[tauri::command]
pub async fn add_to_failover_queue(
    state: tauri::State<'_, AppState>,
    app_type: String,
    provider_id: String,
) -> Result<(), String> {
    require_failover_app(&app_type)?;
    require_failover_provider(&state.db, &app_type, &provider_id)?;
    state
        .db
        .add_to_failover_queue(&app_type, &provider_id)
        .map_err(|e| e.to_string())
}

/// 从故障转移队列移除供应商
#[tauri::command]
pub async fn remove_from_failover_queue(
    state: tauri::State<'_, AppState>,
    app_type: String,
    provider_id: String,
) -> Result<(), String> {
    require_failover_app(&app_type)?;
    state
        .db
        .remove_from_failover_queue(&app_type, &provider_id)
        .map_err(|e| e.to_string())
}

/// 获取指定应用的自动故障转移开关状态（从 proxy_config 表读取）
#[tauri::command]
pub async fn get_auto_failover_enabled(
    state: tauri::State<'_, AppState>,
    app_type: String,
) -> Result<bool, String> {
    require_failover_app(&app_type)?;
    state
        .db
        .get_proxy_config_for_app(&app_type)
        .await
        .map(|config| config.auto_failover_enabled)
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn get_failover_policy(
    state: tauri::State<'_, AppState>,
    app_type: String,
) -> Result<crate::proxy::types::FailoverPolicy, String> {
    require_failover_app(&app_type)?;
    state
        .db
        .get_failover_policy(&app_type)
        .map_err(|error| error.to_string())
}

#[tauri::command]
pub async fn update_failover_policy(
    state: tauri::State<'_, AppState>,
    app_type: String,
    policy: crate::proxy::types::FailoverPolicy,
) -> Result<(), String> {
    require_failover_app(&app_type)?;
    state
        .db
        .set_failover_policy(&app_type, &policy)
        .map_err(|error| error.to_string())
}

/// 设置指定应用的自动故障转移开关状态（写入 proxy_config 表）
///
/// 注意：关闭故障转移时不会清除队列，队列内容会保留供下次开启时使用
#[tauri::command]
pub async fn set_auto_failover_enabled(
    app: tauri::AppHandle,
    state: tauri::State<'_, AppState>,
    app_type: String,
    enabled: bool,
) -> Result<(), String> {
    require_failover_app(&app_type)?;
    log::info!(
        "[Failover] Setting auto_failover_enabled: app_type='{app_type}', enabled={enabled}"
    );

    // 读取当前配置
    let mut config = state
        .db
        .get_proxy_config_for_app(&app_type)
        .await
        .map_err(|e| e.to_string())?;

    let app_enum = crate::app_config::AppType::from_str(&app_type)
        .map_err(|_| format!("无效的应用类型: {app_type}"))?;
    let is_claude_desktop = matches!(app_enum, crate::app_config::AppType::ClaudeDesktop);

    if enabled && !config.enabled && !is_claude_desktop {
        return Err("需要先启用该应用的代理接管，再开启故障转移".to_string());
    }

    if enabled && is_claude_desktop && !state.proxy_service.is_running().await {
        return Err("需要先启动本地路由服务，再开启 Claude Desktop 故障转移".to_string());
    }

    let policy = state
        .db
        .get_failover_policy(&app_type)
        .map_err(|error| error.to_string())?;
    let sticky_rotation = matches!(
        policy.strategy,
        crate::proxy::types::FailoverStrategy::StickyRotation
    );

    // 固定优先级模式在队列为空时自动加入当前供应商作为 P1；当前优先轮转模式
    // 始终确保当前供应商在环形队列中，但不会主动切换到列表第一项。
    let mut auto_added_provider_id: Option<String> = None;
    let activation_provider_id = if enabled {
        let all_providers = state
            .db
            .get_all_providers(&app_type)
            .map_err(|e| e.to_string())?;
        let mut queue = state
            .db
            .get_failover_queue(&app_type)
            .map_err(|e| e.to_string())?
            .into_iter()
            .filter(|item| {
                all_providers
                    .get(&item.provider_id)
                    .is_some_and(|provider| {
                        crate::proxy::provider_router::provider_supports_failover(
                            &app_type, provider,
                        )
                    })
            })
            .collect::<Vec<_>>();

        let current_id = crate::settings::get_effective_current_provider(&state.db, &app_enum)
            .map_err(|e| e.to_string())?;

        if sticky_rotation {
            let Some(current_id) = current_id else {
                return Err("未设置当前供应商，无法开启当前优先轮转".to_string());
            };
            require_failover_provider(&state.db, &app_type, &current_id)?;
            if !queue.iter().any(|item| item.provider_id == current_id) {
                state
                    .db
                    .add_to_failover_queue(&app_type, &current_id)
                    .map_err(|e| e.to_string())?;
                auto_added_provider_id = Some(current_id);
            }
            None
        } else {
            if queue.is_empty() {
                let Some(current_id) = current_id else {
                    return Err(
                        "故障转移队列为空，且未设置当前供应商，无法开启故障转移".to_string()
                    );
                };

                require_failover_provider(&state.db, &app_type, &current_id)?;

                state
                    .db
                    .add_to_failover_queue(&app_type, &current_id)
                    .map_err(|e| e.to_string())?;
                auto_added_provider_id = Some(current_id);

                queue = state
                    .db
                    .get_failover_queue(&app_type)
                    .map_err(|e| e.to_string())?
                    .into_iter()
                    .filter(|item| {
                        all_providers
                            .get(&item.provider_id)
                            .is_some_and(|provider| {
                                crate::proxy::provider_router::provider_supports_failover(
                                    &app_type, provider,
                                )
                            })
                    })
                    .collect();
            }

            Some(
                queue
                    .first()
                    .map(|item| item.provider_id.clone())
                    .ok_or_else(|| "故障转移队列为空，无法开启故障转移".to_string())?,
            )
        }
    } else {
        None
    };

    // 开启前先切到 P1。只有切换成功后才写入 auto_failover_enabled=true，
    // 避免 P1 不可切换（例如 official provider）时留下“开关已开但目标未切”的脏状态。
    if let Some(p1_provider_id) = activation_provider_id.as_deref() {
        let switch_result = if is_claude_desktop {
            crate::services::ProviderService::switch(
                state.inner(),
                crate::app_config::AppType::ClaudeDesktop,
                p1_provider_id,
            )
            .map(|_| ())
            .map_err(|error| error.to_string())
        } else {
            state
                .proxy_service
                .switch_proxy_target(&app_type, p1_provider_id)
                .await
        };
        if let Err(e) = switch_result {
            if let Some(provider_id) = auto_added_provider_id {
                let _ = state.db.remove_from_failover_queue(&app_type, &provider_id);
            }
            return Err(e);
        }
    }

    // 更新 auto_failover_enabled 字段
    config.auto_failover_enabled = enabled;

    // 写回数据库
    state
        .db
        .update_proxy_config_for_app(config)
        .await
        .map_err(|e| e.to_string())?;

    if let Some(provider_id) = activation_provider_id {
        // 发射 provider-switched 事件（让前端刷新当前供应商）
        let event_data = serde_json::json!({
            "appType": app_type,
            "providerId": provider_id,
            "source": "failoverEnabled"
        });
        let _ = app.emit("provider-switched", event_data);
    }

    // 刷新托盘菜单，确保状态同步
    if let Ok(new_menu) = crate::tray::create_tray_menu(&app, &state) {
        if let Some(tray) = app.tray_by_id(crate::tray::TRAY_ID) {
            let _ = tray.set_menu(Some(new_menu));
        }
    }

    Ok(())
}
