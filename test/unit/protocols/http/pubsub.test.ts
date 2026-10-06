/*
Copyright 2026 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

import HTTPClient from "../../../../src/implementation/Client/HTTPClient/HTTPClient";
import HTTPClientPubSub from "../../../../src/implementation/Client/HTTPClient/pubsub";
import { LoggerOptions } from "../../../../src/types/logger/LoggerOptions";

const httpError = (status: number, errorMsg = "") =>
  new Error(JSON.stringify({ error: "Not Found", error_msg: errorMsg, status }));

const getLogger = () => ({
  service: { error: jest.fn(), warn: jest.fn(), info: jest.fn(), verbose: jest.fn(), debug: jest.fn() },
});

// Lets every pending publish run up to its first round trip to the sidecar.
const flushPromises = () => new Promise((resolve) => setImmediate(resolve));

describe("http/pubsub", () => {
  const messages = [{ hello: "world" }, { hello: "world 2" }];

  const getPubSub = (logger?: LoggerOptions) => {
    const client = new HTTPClient({
      daprHost: "",
      daprPort: "",
      communicationProtocol: 0,
      logger,
    });
    const execute = jest.spyOn(client, "executeWithApiVersion");
    return { pubsub: new HTTPClientPubSub(client), execute };
  };

  const publishConcurrently = (pubsub: HTTPClientPubSub, count = 3) =>
    Promise.all(Array.from({ length: count }, () => pubsub.publishBulk("my-pubsub", "my-topic", messages)));

  describe("publishBulk should prefer the stable v1.0 endpoint", () => {
    it("should call the stable v1.0 bulk publish endpoint", async () => {
      const { pubsub, execute } = getPubSub();
      execute.mockResolvedValue({});

      const res = await pubsub.publishBulk("my-pubsub", "my-topic", messages);

      expect(res.failedMessages.length).toBe(0);
      expect(execute).toHaveBeenCalledTimes(1);
      const [apiVersion, path] = execute.mock.calls[0];
      expect(apiVersion).toBe("v1.0");
      expect(path).toContain("/publish/bulk/my-pubsub/my-topic");
    });

    it("should fall back to v1.0-alpha1 when the stable endpoint is missing", async () => {
      const { pubsub, execute } = getPubSub();
      execute.mockRejectedValueOnce(httpError(404)).mockResolvedValueOnce({});

      const res = await pubsub.publishBulk("my-pubsub", "my-topic", messages);

      expect(res.failedMessages.length).toBe(0);
      expect(execute).toHaveBeenCalledTimes(2);
      expect(execute.mock.calls[0][0]).toBe("v1.0");
      expect(execute.mock.calls[1][0]).toBe("v1.0-alpha1");
      expect(execute.mock.calls[1][1]).toContain("/publish/bulk/my-pubsub/my-topic");
    });

    it("should only probe the stable endpoint once when falling back", async () => {
      const { pubsub, execute } = getPubSub();
      execute.mockRejectedValueOnce(httpError(404)).mockResolvedValue({});

      await pubsub.publishBulk("my-pubsub", "my-topic", messages);
      await pubsub.publishBulk("my-pubsub", "my-topic", messages);

      expect(execute).toHaveBeenCalledTimes(3);
      expect(execute.mock.calls.map((call) => call[0])).toEqual(["v1.0", "v1.0-alpha1", "v1.0-alpha1"]);
    });

    it("should probe the stable endpoint once when concurrent calls fall back", async () => {
      const logger = getLogger();
      const { pubsub, execute } = getPubSub(logger);
      execute.mockImplementation(async (apiVersion) => {
        if (apiVersion === "v1.0") {
          throw httpError(404);
        }
        return {};
      });

      const results = await publishConcurrently(pubsub);

      expect(results.map((res) => res.failedMessages.length)).toEqual([0, 0, 0]);
      expect(execute.mock.calls.map((call) => call[0])).toEqual(["v1.0", "v1.0-alpha1", "v1.0-alpha1", "v1.0-alpha1"]);
      expect(logger.service.warn).toHaveBeenCalledTimes(1);
    });

    it("should hold concurrent calls until the first call has probed the stable endpoint", async () => {
      const { pubsub, execute } = getPubSub();
      let completeProbe!: (res: object) => void;
      execute
        .mockImplementationOnce(
          () =>
            new Promise((resolve) => {
              completeProbe = resolve;
            }),
        )
        .mockResolvedValue({});

      const pending = publishConcurrently(pubsub);
      await flushPromises();
      expect(execute).toHaveBeenCalledTimes(1);

      completeProbe({});
      const results = await pending;

      expect(results.map((res) => res.failedMessages.length)).toEqual([0, 0, 0]);
      expect(execute.mock.calls.map((call) => call[0])).toEqual(["v1.0", "v1.0", "v1.0"]);
    });

    it("should not fail concurrent calls when the first call fails for another reason", async () => {
      const { pubsub, execute } = getPubSub();
      execute.mockRejectedValueOnce(httpError(500)).mockResolvedValue({});

      const results = await publishConcurrently(pubsub);

      expect(results.map((res) => res.failedMessages.length)).toEqual([2, 0, 0]);
      expect(execute.mock.calls.map((call) => call[0])).toEqual(["v1.0", "v1.0", "v1.0"]);
    });

    it("should log the fallback warning once when several calls discover it", async () => {
      const logger = getLogger();
      const { pubsub, execute } = getPubSub(logger);
      // The first call never reaches the sidecar, so the calls waiting on it
      // each find out for themselves that the stable endpoint is missing.
      execute.mockRejectedValueOnce(new Error("socket hang up")).mockImplementation(async (apiVersion) => {
        if (apiVersion === "v1.0") {
          throw httpError(404);
        }
        return {};
      });

      const results = await publishConcurrently(pubsub);

      expect(results.map((res) => res.failedMessages.length)).toEqual([2, 0, 0]);
      expect(logger.service.warn).toHaveBeenCalledTimes(1);

      execute.mockClear();
      await pubsub.publishBulk("my-pubsub", "my-topic", messages);

      expect(execute.mock.calls.map((call) => call[0])).toEqual(["v1.0-alpha1"]);
    });

    it("should remember the fallback when the alpha1 endpoint reports failed entries", async () => {
      const logger = getLogger();
      const { pubsub, execute } = getPubSub(logger);
      execute
        .mockRejectedValueOnce(httpError(404))
        .mockRejectedValueOnce(
          new Error(
            JSON.stringify({
              error: "Internal Server Error",
              error_msg: JSON.stringify({ failedEntries: [{ entryID: "1", error: "failed to publish" }] }),
              status: 500,
            }),
          ),
        )
        .mockResolvedValue({});

      const res = await pubsub.publishBulk("my-pubsub", "my-topic", [
        { entryID: "1", event: { hello: "world" }, contentType: "application/json" },
        { entryID: "2", event: { hello: "world 2" }, contentType: "application/json" },
      ]);

      expect(res.failedMessages.map((failed) => failed.message.entryID)).toEqual(["1"]);
      expect(logger.service.warn).toHaveBeenCalledTimes(1);

      execute.mockClear();
      await pubsub.publishBulk("my-pubsub", "my-topic", messages);

      expect(execute.mock.calls.map((call) => call[0])).toEqual(["v1.0-alpha1"]);
    });

    it("should not fall back when the stable endpoint fails for another reason", async () => {
      const { pubsub, execute } = getPubSub();
      execute.mockRejectedValue(httpError(500));

      const res = await pubsub.publishBulk("my-pubsub", "my-topic", messages);

      expect(execute).toHaveBeenCalledTimes(1);
      expect(res.failedMessages.length).toBe(2);
    });

    it("should not remember the fallback when the alpha1 endpoint also returns 404", async () => {
      const { pubsub, execute } = getPubSub();
      execute.mockRejectedValue(httpError(404));

      // A 404 from both endpoints means the pub/sub component was not found,
      // not that the runtime predates the stable endpoint.
      const res = await pubsub.publishBulk("my-pubsub", "my-topic", messages);
      expect(res.failedMessages.length).toBe(2);

      execute.mockClear();
      execute.mockResolvedValue({});
      await pubsub.publishBulk("my-pubsub", "my-topic", messages);

      expect(execute.mock.calls[0][0]).toBe("v1.0");
    });

    it("should map failed entries reported by the bulk publish endpoint", async () => {
      const { pubsub, execute } = getPubSub();
      execute.mockRejectedValue(
        new Error(
          JSON.stringify({
            error: "Internal Server Error",
            error_msg: JSON.stringify({ failedEntries: [{ entryID: "1", error: "failed to publish" }] }),
            status: 500,
          }),
        ),
      );

      const res = await pubsub.publishBulk("my-pubsub", "my-topic", [
        { entryID: "1", event: { hello: "world" }, contentType: "application/json" },
        { entryID: "2", event: { hello: "world 2" }, contentType: "application/json" },
      ]);

      expect(res.failedMessages.length).toBe(1);
      expect(res.failedMessages[0].message.entryID).toBe("1");
      expect(res.failedMessages[0].error.message).toBe("failed to publish");
    });
  });
});
