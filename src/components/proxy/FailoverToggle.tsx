/**
 * 故障转移切换开关组件
 *
 * 放置在主界面头部，用于一键启用/关闭自动故障转移
 */

import { Shuffle, Loader2 } from "lucide-react";
import { Switch } from "@/components/ui/switch";
import {
  useAutoFailoverEnabled,
  useFailoverPolicy,
  useSetAutoFailoverEnabled,
} from "@/lib/query/failover";
import { useProxyStatus } from "@/hooks/useProxyStatus";
import { cn } from "@/lib/utils";
import { useTranslation } from "react-i18next";
import { getAppLabel, type FailoverAppId } from "@/config/appConfig";

interface FailoverToggleProps {
  className?: string;
  activeApp: FailoverAppId;
}

export function FailoverToggle({ className, activeApp }: FailoverToggleProps) {
  const { t } = useTranslation();
  const { data: isEnabled = false, isLoading } =
    useAutoFailoverEnabled(activeApp);
  const { data: policy } = useFailoverPolicy(activeApp);
  const setEnabled = useSetAutoFailoverEnabled();
  const { takeoverStatus, isRunning } = useProxyStatus();
  const routingReady =
    activeApp === "claude-desktop"
      ? isRunning
      : (takeoverStatus?.[activeApp] ?? false);
  const isStickyRotation = policy?.strategy === "stickyRotation";

  const handleToggle = (checked: boolean) => {
    if (checked && !routingReady) return;
    setEnabled.mutate({ appType: activeApp, enabled: checked });
  };

  const appLabel = getAppLabel(activeApp);

  const tooltipText = !routingReady
    ? activeApp === "claude-desktop"
      ? t("failover.tooltip.routingRequired", {
          app: appLabel,
          defaultValue: `请先启动本地路由服务，再启用 ${appLabel} 故障转移`,
        })
      : t("failover.tooltip.takeoverRequired", {
          app: appLabel,
          defaultValue: `请先接管 ${appLabel}，再启用故障转移`,
        })
    : isEnabled
      ? isStickyRotation
        ? t("failover.tooltip.stickyEnabled", {
            app: appLabel,
            defaultValue: `${appLabel} 当前优先轮转已启用\n当前供应商为动态 P1，失败后环形轮转`,
          })
        : t("failover.tooltip.enabled", {
            app: appLabel,
            defaultValue: `${appLabel} 故障转移已启用\n按队列优先级（P1→P2→...）选择供应商`,
          })
      : isStickyRotation
        ? t("failover.tooltip.stickyDisabled", {
            app: appLabel,
            defaultValue: `启用 ${appLabel} 当前优先轮转\n保留当前供应商，失败后环形切换`,
          })
        : t("failover.tooltip.disabled", {
            app: appLabel,
            defaultValue: `启用 ${appLabel} 故障转移\n将立即切换到队列 P1，并在失败时自动切换到下一个`,
          });

  return (
    <div
      className={cn(
        "flex items-center gap-1 px-1.5 h-8 rounded-lg bg-muted/50 transition-all",
        className,
      )}
      title={tooltipText}
    >
      {setEnabled.isPending || isLoading ? (
        <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
      ) : (
        <Shuffle
          className={cn(
            "h-4 w-4 transition-colors",
            isEnabled
              ? "text-emerald-500 status-heartbeat"
              : "text-muted-foreground",
          )}
        />
      )}
      <Switch
        checked={isEnabled}
        onCheckedChange={handleToggle}
        disabled={setEnabled.isPending || isLoading || !routingReady}
      />
    </div>
  );
}
