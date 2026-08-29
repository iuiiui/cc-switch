import { render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { FailoverToggle } from "@/components/proxy/FailoverToggle";

const queryMocks = vi.hoisted(() => ({
  useAutoFailoverEnabled: vi.fn(),
  useFailoverPolicy: vi.fn(),
  useSetAutoFailoverEnabled: vi.fn(),
}));
const useProxyStatusMock = vi.hoisted(() => vi.fn());

vi.mock("@/lib/query/failover", () => queryMocks);
vi.mock("@/hooks/useProxyStatus", () => ({
  useProxyStatus: useProxyStatusMock,
}));

describe("FailoverToggle", () => {
  beforeEach(() => {
    queryMocks.useAutoFailoverEnabled.mockReturnValue({
      data: false,
      isLoading: false,
    });
    queryMocks.useFailoverPolicy.mockReturnValue({
      data: { strategy: "stickyRotation" },
    });
    queryMocks.useSetAutoFailoverEnabled.mockReturnValue({
      mutate: vi.fn(),
      isPending: false,
    });
    useProxyStatusMock.mockReturnValue({
      isRunning: true,
      takeoverStatus: undefined,
    });
  });

  it("uses routing-service readiness for Claude Desktop", () => {
    const { rerender } = render(<FailoverToggle activeApp="claude-desktop" />);
    expect(screen.getByRole("switch")).toBeEnabled();

    useProxyStatusMock.mockReturnValue({
      isRunning: false,
      takeoverStatus: undefined,
    });
    rerender(<FailoverToggle activeApp="claude-desktop" />);
    expect(screen.getByRole("switch")).toBeDisabled();
  });
});
