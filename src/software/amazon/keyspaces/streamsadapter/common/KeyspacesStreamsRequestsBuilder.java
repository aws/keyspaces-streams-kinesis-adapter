/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package software.amazon.keyspaces.streamsadapter.common;

import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.ApiName;
import software.amazon.awssdk.services.keyspacesstreams.model.GetStreamRequest;
import software.amazon.awssdk.services.keyspacesstreams.model.GetRecordsRequest;
import software.amazon.awssdk.services.keyspacesstreams.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.keyspacesstreams.model.ListStreamsRequest;
import software.amazon.kinesis.retrieval.RetrievalConfig;

/**
 * Builder class for Keyspaces Streams requests with user agent information.
 */
public final class KeyspacesStreamsRequestsBuilder {

    /**
     * Private constructor to prevent instantiation of this utility class.
     */
    private KeyspacesStreamsRequestsBuilder() {
        // Utility class, do not instantiate
    }

    /**
     * Creates a builder for ListStreams request with user agent information.
     *
     * @return ListStreamsRequest builder with user agent configuration
     */
    public static ListStreamsRequest.Builder listStreamsRequestBuilder() {
        return appendUserAgent(ListStreamsRequest.builder());
    }

    /**
     * Creates a builder for GetRecords request with user agent information.
     *
     * @return GetRecordsRequest builder with user agent configuration
     */
    public static GetRecordsRequest.Builder getRecordsRequestBuilder() {
        return appendUserAgent(GetRecordsRequest.builder());
    }

    /**
     * Creates a builder for GetShardIterator request with user agent information.
     *
     * @return GetShardIteratorRequest builder with user agent configuration
     */
    public static GetShardIteratorRequest.Builder getShardIteratorRequestBuilder() {
        return appendUserAgent(GetShardIteratorRequest.builder());
    }

    /**
     * Creates a builder for GetStream request with user agent information.
     *
     * @return GetStreamRequest builder with user agent configuration
     */
    public static GetStreamRequest.Builder getStreamRequestBuilder() {
        return appendUserAgent(GetStreamRequest.builder());
    }

    @SuppressWarnings("unchecked")
    private static <T extends AwsRequest.Builder> T appendUserAgent(final T builder) {
        return (T) builder.overrideConfiguration(AwsRequestOverrideConfiguration.builder()
                .addApiName(ApiName.builder()
                        .name(RetrievalConfig.KINESIS_CLIENT_LIB_USER_AGENT)
                        .version(RetrievalConfig.KINESIS_CLIENT_LIB_USER_AGENT_VERSION)
                        .build())
                .build());
    }
}