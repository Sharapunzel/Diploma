import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiClient } from "./client";
import { DetectorsApi } from "./detectors";
const id = "00000000-0000-4000-8000-000000000123";
afterEach(() => vi.unstubAllGlobals());
describe("detector transport", () => {
  it.each([200, 201, 204, 205])(
    "rejects empty successful %s DTO",
    async (status) => {
      vi.stubGlobal(
        "fetch",
        vi.fn().mockResolvedValue(
          new Response(status === 204 || status === 205 ? null : "", {
            status,
          }),
        ),
      );
      await expect(
        new DetectorsApi(new ApiClient()).command("start", id, {}, "key"),
      ).rejects.toMatchObject({ code: "invalid_response" });
    },
  );
  it("does not automatically replay CSRF-refused mutations", async () => {
    const fetch = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          code: "csrf_invalid",
          message: "secret",
          request_id: "rid",
          details: {},
        }),
        { status: 403 },
      ),
    );
    vi.stubGlobal("fetch", fetch);
    const client = new ApiClient(),
      refresh = vi.fn().mockResolvedValue(undefined);
    client.setAuthHandlers({ unauthorized: vi.fn(), csrfInvalid: refresh });
    await expect(
      new DetectorsApi(client).command("stop", id, {}, "same"),
    ).rejects.toMatchObject({ status: 403, requestId: "rid" });
    expect(fetch).toHaveBeenCalledTimes(1);
    expect(refresh).toHaveBeenCalledTimes(1);
  });
});
