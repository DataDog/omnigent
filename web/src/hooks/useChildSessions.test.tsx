import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import type { PropsWithChildren } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { authenticatedFetch } from "@/lib/identity";
import {
  childSessionsQueryKey,
  fetchChildSessions,
  refreshChildSessionRosters,
  useChildSessions,
  useDescendantSession,
} from "./useChildSessions";

vi.mock("@/lib/identity", () => ({
  authenticatedFetch: vi.fn(),
}));

function wrapper({ children }: PropsWithChildren) {
  return (
    <QueryClientProvider
      client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}
    >
      {children}
    </QueryClientProvider>
  );
}

function wrapperFor(client: QueryClient) {
  return function QueryClientWrapper({ children }: PropsWithChildren) {
    return <QueryClientProvider client={client}>{children}</QueryClientProvider>;
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

function childListResponse(sourceCurrent: boolean): Response {
  return new Response(
    JSON.stringify({
      object: "list",
      data: [
        {
          id: "conv_acp_nested_worker",
          title: "worker:nested",
          tool: "worker",
          session_name: "nested",
          current_task_status: sourceCurrent ? "in_progress" : null,
          busy: sourceCurrent,
          acp_session_observation: {
            source_current: sourceCurrent,
            state: { present: true, value: { state: "running" } },
          },
        },
        {
          id: "conv_ordinary_worker",
          title: "researcher:ordinary",
          tool: "researcher",
          session_name: "ordinary",
          current_task_status: "completed",
          busy: false,
          acp_session_observation: null,
        },
      ],
    }),
  );
}

beforeEach(() => {
  vi.mocked(authenticatedFetch).mockReset();
});

describe("useChildSessions", () => {
  it("does not fetch child sessions for a provisional conversation", () => {
    const { result } = renderHook(() => useChildSessions("temp:pending-create"), { wrapper });

    expect(result.current.children).toEqual([]);
    expect(authenticatedFetch).not.toHaveBeenCalled();
  });

  it("preserves ACP observation fields and defaults missing source eligibility to false", async () => {
    const currentObservation = {
      source_current: true,
      state: {
        present: true,
        value: { state: "idle", stopReason: "unknown future reason" },
      },
      state_ingress_sequence: 17,
      transport: { state: "disconnected", reason: "pipe closed" },
    };
    const sparseObservation = {
      state: { present: true, value: { state: "running" } },
      state_ingress_sequence: 9,
    };
    vi.mocked(authenticatedFetch).mockResolvedValueOnce(
      new Response(
        JSON.stringify({
          object: "list",
          data: [
            {
              id: "conv_acp_current",
              title: "worker:current",
              tool: "worker",
              session_name: "current",
              current_task_status: null,
              busy: false,
              acp_session_observation: currentObservation,
            },
            {
              id: "conv_acp_sparse",
              title: "worker:sparse",
              tool: "worker",
              session_name: "sparse",
              current_task_status: null,
              busy: false,
              acp_session_observation: sparseObservation,
            },
            {
              id: "conv_ordinary",
              title: "researcher:auth",
              tool: "researcher",
              session_name: "auth",
              current_task_status: "completed",
              busy: false,
              acp_session_observation: null,
            },
          ],
        }),
      ),
    );

    const children = await fetchChildSessions("conv_parent");

    expect(children[0].acp_session_observation).toEqual(currentObservation);
    expect(children[1].acp_session_observation).toEqual({
      ...sparseObservation,
      source_current: false,
    });
    expect(children[2].acp_session_observation).toBeNull();
  });

  it("cancels an older roster GET and refreshes active, inactive, and empty nested entries", async () => {
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false, staleTime: Infinity } },
    });
    const nestedParent = "conv_nested_parent_without_cached_ancestor";
    const inactiveParent = "conv_inactive_parent";
    const emptyParent = "conv_empty_parent";
    const nestedKey = childSessionsQueryKey(nestedParent);
    const inactiveKey = childSessionsQueryKey(inactiveParent);
    const emptyKey = childSessionsQueryKey(emptyParent);
    const oldNestedResponse = deferred<Response>();
    const oldEmptyResponse = deferred<Response>();
    const requestCounts = new Map<string, number>();
    let nestedFirstSignal: AbortSignal | undefined;
    let emptyFirstSignal: AbortSignal | undefined;

    vi.mocked(authenticatedFetch).mockImplementation((input, init) => {
      const sessionId = decodeURIComponent(String(input).split("/").at(-2) ?? "");
      const requestNumber = (requestCounts.get(sessionId) ?? 0) + 1;
      requestCounts.set(sessionId, requestNumber);
      if (sessionId === nestedParent && requestNumber === 1) {
        nestedFirstSignal = init?.signal ?? undefined;
        return oldNestedResponse.promise;
      }
      if (sessionId === emptyParent && requestNumber === 1) {
        emptyFirstSignal = init?.signal ?? undefined;
        return oldEmptyResponse.promise;
      }
      return Promise.resolve(childListResponse(false));
    });

    const nestedQueryFn = ({ signal }: { signal: AbortSignal }) =>
      fetchChildSessions(nestedParent, signal);
    const inactiveQueryFn = ({ signal }: { signal: AbortSignal }) =>
      fetchChildSessions(inactiveParent, signal);
    const emptyQueryFn = ({ signal }: { signal: AbortSignal }) =>
      fetchChildSessions(emptyParent, signal);
    queryClient.setQueryDefaults(nestedKey, { queryFn: nestedQueryFn });
    queryClient.setQueryDefaults(inactiveKey, { queryFn: inactiveQueryFn });
    queryClient.setQueryDefaults(emptyKey, { queryFn: emptyQueryFn });
    queryClient.setQueryData(nestedKey, [
      {
        id: "conv_acp_nested_worker",
        title: "worker:nested",
        task_summary: null,
        tool: "worker",
        session_name: "nested",
        labels: {},
        current_task_status: "in_progress",
        busy: true,
        last_message_preview: null,
        pending_elicitations_count: 0,
        routed_model: null,
        acp_session_observation: {
          source_current: true,
          state: { present: true, value: { state: "running" } },
        },
      },
    ]);
    queryClient.setQueryData(inactiveKey, [
      {
        id: "conv_cached_acp_inactive",
        acp_session_observation: {
          source_current: true,
          state: { present: true, value: { state: "requires_action" } },
        },
      },
    ]);

    const initialEmptyFetch = queryClient.fetchQuery({
      queryKey: emptyKey,
      queryFn: emptyQueryFn,
    });
    void initialEmptyFetch.catch(() => undefined);
    await waitFor(() => expect(requestCounts.get(emptyParent)).toBe(1));

    const { result } = renderHook(() => useChildSessions(nestedParent), {
      wrapper: wrapperFor(queryClient),
    });
    await waitFor(() => expect(result.current.children).toHaveLength(1));

    const firstRefresh = refreshChildSessionRosters(queryClient);
    await waitFor(() => expect(requestCounts.get(nestedParent)).toBe(1));
    expect(nestedFirstSignal?.aborted).toBe(false);

    const secondRefresh = refreshChildSessionRosters(queryClient);
    expect(nestedFirstSignal?.aborted).toBe(true);
    expect(emptyFirstSignal?.aborted).toBe(true);
    await waitFor(() => expect(requestCounts.get(nestedParent)).toBe(2));
    await Promise.all([firstRefresh, secondRefresh]);

    const lateNestedResponse = childListResponse(true);
    const lateEmptyResponse = childListResponse(true);
    oldNestedResponse.resolve(lateNestedResponse);
    oldEmptyResponse.resolve(lateEmptyResponse);
    await waitFor(() => {
      expect(lateNestedResponse.bodyUsed).toBe(true);
      expect(lateEmptyResponse.bodyUsed).toBe(true);
    });

    // A canceled, never-mounted initial query is disabled once its request
    // settles. Its late response must not repopulate the cache; mounting the
    // roster later performs the authoritative stale-on-mount read.
    expect(queryClient.getQueryData(emptyKey)).toBeUndefined();
    const { result: emptyResult } = renderHook(() => useChildSessions(emptyParent), {
      wrapper: wrapperFor(queryClient),
    });
    await waitFor(() => {
      expect(requestCounts.get(emptyParent)).toBe(2);
      expect(emptyResult.current.children).toHaveLength(2);
    });

    await waitFor(() => {
      expect(result.current.children[0]?.acp_session_observation?.source_current).toBe(false);
    });
    expect(requestCounts.get(inactiveParent)).toBeGreaterThanOrEqual(2);
    expect(
      queryClient.getQueryData<Awaited<ReturnType<typeof fetchChildSessions>>>(nestedKey)?.[0],
    ).toMatchObject({
      busy: false,
      current_task_status: null,
      acp_session_observation: {
        source_current: false,
        state: { present: true, value: { state: "running" } },
      },
    });
    expect(
      queryClient.getQueryData<Awaited<ReturnType<typeof fetchChildSessions>>>(inactiveKey)?.[0],
    ).toMatchObject({
      busy: false,
      acp_session_observation: {
        source_current: false,
        state: { present: true, value: { state: "running" } },
      },
    });
    expect(emptyResult.current.children[0]?.acp_session_observation?.source_current).toBe(false);
    expect(emptyResult.current.children[1]).toMatchObject({
      id: "conv_ordinary_worker",
      current_task_status: "completed",
      acp_session_observation: null,
    });
    queryClient.clear();
  });
});

describe("useDescendantSession", () => {
  it("fetches the direct-child list when it is not cached", async () => {
    // WHY: a mirrored card can mount before anything loaded the ancestor's
    // child list; the lookup must fetch it rather than stay unlabeled.
    vi.mocked(authenticatedFetch).mockResolvedValueOnce(
      new Response(
        JSON.stringify({
          object: "list",
          data: [
            {
              id: "conv_child",
              title: "researcher:auth",
              task_summary: "Investigate auth flow",
              tool: "researcher",
              session_name: "auth",
              current_task_status: "in_progress",
              busy: true,
            },
          ],
        }),
      ),
    );
    const { result } = renderHook(() => useDescendantSession("conv_parent", "conv_child"), {
      wrapper,
    });

    expect(result.current).toBeNull();
    await waitFor(() => expect(result.current?.task_summary).toBe("Investigate auth flow"));
    expect(authenticatedFetch).toHaveBeenCalledWith("/v1/sessions/conv_parent/child_sessions", {
      signal: expect.any(AbortSignal),
    });
  });
});
