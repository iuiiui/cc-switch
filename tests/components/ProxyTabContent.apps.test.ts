import { describe, expect, it } from "vitest";
import { FAILOVER_APPS } from "@/components/settings/ProxyTabContent";

describe("ProxyTabContent failover apps", () => {
  it("includes Claude Desktop's dedicated local gateway", () => {
    expect(FAILOVER_APPS.map(({ id }) => id)).toEqual([
      "claude",
      "claude-desktop",
      "codex",
      "gemini",
      "grokbuild",
    ]);
  });
});
