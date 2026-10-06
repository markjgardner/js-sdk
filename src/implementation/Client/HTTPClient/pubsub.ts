/*
Copyright 2022 The Dapr Authors
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

import HTTPClient from "./HTTPClient";
import IClientPubSub from "../../../interfaces/Client/IClientPubSub";
import { Logger } from "../../../logger/Logger";
import { KeyValueType } from "../../../types/KeyValue.type";
import { createHTTPQueryParam, getBulkPublishEntries, getBulkPublishResponse } from "../../../utils/Client.util";
import { THTTPExecuteParams } from "../../../types/http/THTTPExecuteParams.type";
import { PubSubBulkPublishResponse } from "../../../types/pubsub/PubSubBulkPublishResponse.type";
import { PubSubBulkPublishMessage } from "../../../types/pubsub/PubSubBulkPublishMessage.type";
import { PubSubBulkPublishEntry } from "../../../types/pubsub/PubSubBulkPublishEntry.type";
import { PubSubPublishResponseType } from "../../../types/pubsub/PubSubPublishResponse.type";
import { PubSubPublishOptions } from "../../../types/pubsub/PubSubPublishOptions.type";

/**
 * Extracts the HTTP status code from an error raised by {@link HTTPClient.execute}.
 * Error responses are serialized as a JSON payload carrying the status code, so
 * an error that does not parse, such as a network failure, has none.
 */
function getHttpStatus(error: any): number | undefined {
  try {
    const status = JSON.parse(error?.message)?.status;
    return typeof status === "number" ? status : undefined;
  } catch (_e: any) {
    return undefined;
  }
}

// https://docs.dapr.io/reference/api/pubsub_api/
export default class HTTPClientPubSub implements IClientPubSub {
  client: HTTPClient;
  private readonly logger: Logger;

  /**
   * Set once the stable bulk publish endpoint has been confirmed absent from
   * the sidecar, so subsequent calls go straight to the alpha1 endpoint
   * instead of paying for a failed round trip each time.
   */
  private useBulkPublishAlpha1 = false;

  /**
   * Settles once the first bulk publish on this client has completed, by which
   * point it has found out whether the sidecar serves the stable endpoint.
   * Calls made in the meantime wait on it rather than each probing the
   * sidecar, so however many publishes start concurrently, a pre-1.17 sidecar
   * normally costs one failed round trip and one warning per client. It never
   * rejects.
   */
  private bulkPublishProbe?: Promise<void>;

  constructor(client: HTTPClient) {
    this.client = client;
    this.logger = new Logger("HTTPClient", "PubSub", client.options.logger);
  }

  async publish(
    pubSubName: string,
    topic: string,
    data: object | string,
    options: PubSubPublishOptions = {},
  ): Promise<PubSubPublishResponseType> {
    const queryParams = createHTTPQueryParam({ data: options.metadata, type: "metadata" });

    // Set content type if provided.
    // If not, HTTPClient will infer it from the data.
    const headers: KeyValueType = {};
    if (options.contentType) {
      headers["Content-Type"] = options.contentType;
    }

    try {
      await this.client.execute(`/publish/${pubSubName}/${topic}?${queryParams}`, {
        method: "POST",
        body: data,
        headers,
      });
    } catch (e: any) {
      this.logger.error(`publish failed: ${e}`);
      return { error: e };
    }

    return {};
  }

  async publishBulk(
    pubSubName: string,
    topic: string,
    messages: PubSubBulkPublishMessage[],
    metadata?: KeyValueType | undefined,
  ): Promise<PubSubBulkPublishResponse> {
    const queryParams = createHTTPQueryParam({ data: metadata, type: "metadata" });
    const params: THTTPExecuteParams = {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
    };

    const entries = getBulkPublishEntries(messages);
    params.body = entries;

    const path = `/publish/bulk/${pubSubName}/${topic}?${queryParams}`;

    try {
      await this.executeBulkPublish(path, params);
    } catch (error: any) {
      this.logger.error(`Failure publishing bulk messages: ${error}`);
      return this.handleBulkPublishError(entries, error);
    }

    // If no error is thrown, all messages were published successfully
    return { failedMessages: [] };
  }

  /**
   * Calls the stable `v1.0` bulk publish endpoint, falling back to the
   * deprecated `v1.0-alpha1` endpoint when the sidecar does not expose it.
   *
   * The stable endpoint was introduced in Dapr 1.17, and older sidecars answer
   * it with a 404. A 404 is also how the sidecar reports an unknown pub/sub
   * component, so the fallback is only remembered once the alpha1 endpoint has
   * actually answered, which is the only proof that the runtime is an old one.
   *
   * The first call doubles as the capability probe, and calls that arrive
   * while it is in flight wait for its outcome before sending their own batch
   * to the selected endpoint.
   */
  private async executeBulkPublish(path: string, params: THTTPExecuteParams): Promise<object | string> {
    if (!this.bulkPublishProbe) {
      const probe = this.executeBulkPublishStableFirst(path, params);
      this.bulkPublishProbe = probe.then(
        () => undefined,
        () => undefined,
      );
      return await probe;
    }

    await this.bulkPublishProbe;

    if (this.useBulkPublishAlpha1) {
      return await this.client.executeWithApiVersion("v1.0-alpha1", path, params);
    }

    return await this.executeBulkPublishStableFirst(path, params);
  }

  private async executeBulkPublishStableFirst(path: string, params: THTTPExecuteParams): Promise<object | string> {
    try {
      return await this.client.executeWithApiVersion("v1.0", path, params);
    } catch (error: any) {
      if (getHttpStatus(error) !== 404) {
        throw error;
      }
    }

    try {
      const result = await this.client.executeWithApiVersion("v1.0-alpha1", path, params);
      this.fallBackToBulkPublishAlpha1();
      return result;
    } catch (error: any) {
      // A sidecar that serves the stable endpoint only answers it with a 404
      // when the component is unknown, and then answers alpha1 the same way.
      // Any other answer, such as a 500 for a partially failed batch, means
      // only the stable endpoint is missing.
      const status = getHttpStatus(error);
      if (status !== undefined && status !== 404) {
        this.fallBackToBulkPublishAlpha1();
      }
      throw error;
    }
  }

  private fallBackToBulkPublishAlpha1(): void {
    if (this.useBulkPublishAlpha1) {
      return;
    }

    this.useBulkPublishAlpha1 = true;
    this.logger.warn(
      "The Dapr sidecar does not expose the stable v1.0 bulk publish endpoint, " +
        "falling back to the deprecated v1.0-alpha1 endpoint. Upgrade to Dapr 1.17 or newer.",
    );
  }

  private async handleBulkPublishError(
    entries: PubSubBulkPublishEntry[],
    error: any,
  ): Promise<PubSubBulkPublishResponse> {
    try {
      // If the error is returned by the bulk publish API,
      // parse the error message and return the response
      const err = JSON.parse(error.message);
      if (err.error_msg) {
        const bulkPublishResponse = JSON.parse(err.error_msg);
        return getBulkPublishResponse({ entries: entries, response: bulkPublishResponse });
      }
    } catch (_innerError: any) {
      // This can indicate a general error with the request (e.g., network error, invalid pubsub name, etc.).
    }

    return getBulkPublishResponse({ entries: entries, error: error });
  }
}
