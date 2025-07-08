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

package software.amazon.keyspaces.streamsadapter;

import software.amazon.keyspaces.streamsadapter.adapter.KeyspacesStreamsGetRecordsResponseAdapter;
import software.amazon.keyspaces.streamsadapter.common.KeyspacesStreamsRequestsBuilder;
import software.amazon.keyspaces.streamsadapter.util.AmazonServiceExceptionTransformer;
import software.amazon.keyspaces.streamsadapter.util.Sleeper;
import com.google.common.annotations.VisibleForTesting;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.internal.retry.SdkDefaultRetryStrategy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.retries.StandardRetryStrategy;
import software.amazon.awssdk.retries.api.BackoffStrategy;

import software.amazon.awssdk.services.keyspacesstreams.model.GetStreamRequest;
import software.amazon.awssdk.services.keyspacesstreams.model.GetStreamResponse;
import software.amazon.awssdk.services.keyspacesstreams.model.ResourceNotFoundException;
import software.amazon.awssdk.services.keyspacesstreams.model.ThrottlingException;
import software.amazon.awssdk.services.keyspacesstreams.model.ValidationException;
import software.amazon.awssdk.services.keyspacesstreams.model.ValidationExceptionType;
import software.amazon.awssdk.services.keyspacesstreams.KeyspacesStreamsClient;

import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisServiceClientConfiguration;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.ListStreamsRequest;
import software.amazon.awssdk.services.kinesis.model.ListStreamsResponse;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.kinesis.retrieval.GetRecordsResponseAdapter;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class AmazonKeyspacesStreamsAdapterClient implements KinesisAsyncClient {

    /**
     * Enum values decides the behavior of application when customer loses some records when KCL lags behind
     */
    public enum SkipRecordsBehavior {
        /**
         * Skips processing to the oldest available record
         */
        SKIP_RECORDS_TO_TRIM_HORIZON, /**
         * Throws an exception to KCL, which retries (infinitely) to fetch the data
         */
        KCL_RETRY;
    }

    @Getter
    private SkipRecordsBehavior skipRecordsBehavior = SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON;
    private static int MAX_GET_STREAM_RETRY_ATTEMPTS = 50;
    private static Duration GET_STREAM_CALLS_DELAY = Duration.ofMillis(1000);

    private Region region;

    private final KeyspacesStreamsClient internalClient;
    private final Sleeper sleeper;

    /**
     * Recommended constructor for AmazonKeyspacesStreamsAdapterClient which takes in an AmazonKeyspacesStreams
     * interface. If you need to execute setEndpoint(String,String,String) or setServiceNameIntern() methods,
     * you should do that on KeyspacesStreamsClient implementation before passing it in this constructor.
     *
     * @param credentialsProvider AWS credentials provider
     * @param region AWS region for the client
     */
    public AmazonKeyspacesStreamsAdapterClient(
            AwsCredentialsProvider credentialsProvider,
            Region region) {
        BackoffStrategy backoffStrategy = BackoffStrategy.exponentialDelay(Duration.ofMillis(100),
                Duration.ofMillis(10000));
        StandardRetryStrategy retryStrategy =
                SdkDefaultRetryStrategy.standardRetryStrategy()
                        .toBuilder()
                        .maxAttempts(10)
                        .backoffStrategy(backoffStrategy)
                        .throttlingBackoffStrategy(backoffStrategy)
                        .build();
        internalClient = KeyspacesStreamsClient.builder()
                .credentialsProvider(credentialsProvider)
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .retryStrategy(retryStrategy)
                        .build())
                .region(region)
                .build();
        this.region = region;
        this.sleeper = new Sleeper();
    }

    public AmazonKeyspacesStreamsAdapterClient(
            KeyspacesStreamsClient keyspacesStreamsClient,
            Region region
    ) {
        internalClient = keyspacesStreamsClient;
        this.region = region;
        this.sleeper = new Sleeper();
    }

    @VisibleForTesting
    protected AmazonKeyspacesStreamsAdapterClient(KeyspacesStreamsClient client) {
        this.internalClient = client;
        this.sleeper = new Sleeper();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String serviceName() {
        return internalClient.serviceName();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        internalClient.close();
    }

    /**
     * @param describeStreamRequest Container for the necessary parameters to execute the DescribeStream service method on Keyspaces
     *                              Streams.
     * @return The response from the DescribeStream service method, adapted for use with the AmazonKinesis model.
     * @throws UnsupportedOperationException since the adapter uses getStream
     */
    @Override
    public CompletableFuture<DescribeStreamResponse> describeStream(
            software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest describeStreamRequest)
            throws AwsServiceException, SdkClientException {
        throw new UnsupportedOperationException();
    }

    public CompletableFuture<GetStreamResponse> getStream(
            GetStreamRequest getStreamRequest)
            throws AwsServiceException, SdkClientException {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return getStreamWithRetries(getStreamRequest);
            } catch (AwsServiceException e) {
                throw AmazonServiceExceptionTransformer.transformKeyspacesStreamsToKinesisDescribeStream(e, skipRecordsBehavior);
            }
        });
    }

    /**
     * @param getShardIteratorRequest Container for the necessary parameters to execute the GetShardIterator service method on Keyspaces
     *                                Streams.
     * @return The response from the GetShardIterator service method, adapted for use with the AmazonKinesis model.
     */
    @Override
    public CompletableFuture<GetShardIteratorResponse> getShardIterator(
            GetShardIteratorRequest getShardIteratorRequest)
            throws AwsServiceException, SdkClientException {
        return CompletableFuture.supplyAsync(() -> getShardIteratorResponse(getShardIteratorRequest));
    }

    private GetShardIteratorResponse getShardIteratorResponse(
            GetShardIteratorRequest getShardIteratorRequest
    ) throws AwsServiceException, SdkClientException {
        software.amazon.awssdk.services.keyspacesstreams.model.GetShardIteratorRequest keyspacesGetShardIteratorRequest =
                KeyspacesStreamsRequestsBuilder.getShardIteratorRequestBuilder()
                        .streamArn(getShardIteratorRequest.streamName())
                        .shardId(getShardIteratorRequest.shardId())
                        .shardIteratorType(getShardIteratorRequest.shardIteratorTypeAsString())
                        .sequenceNumber(getShardIteratorRequest.startingSequenceNumber())
                        .build();
        software.amazon.awssdk.services.keyspacesstreams.model.GetShardIteratorResponse result;
        try {
            result = internalClient.getShardIterator(keyspacesGetShardIteratorRequest);
        } catch (ValidationException e) {
            if (e.errorCode() == ValidationExceptionType.TRIMMED_DATA_ACCESS &&
                    skipRecordsBehavior == SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON) {
                if (getShardIteratorRequest.shardIteratorType().equals(
                        software.amazon.awssdk.services.kinesis.model.ShardIteratorType.TRIM_HORIZON)) {
                    throw AmazonServiceExceptionTransformer.transformKeyspacesStreamsToKinesisGetShardIterator(e, skipRecordsBehavior);
                }
                log.warn("Data has been trimmed. Intercepting Keyspaces exception and retrieving a fresh iterator {}", getShardIteratorRequest, e);
                return getShardIteratorResponse(
                        getShardIteratorRequest.toBuilder()
                                .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
                                .startingSequenceNumber(null)
                                .build());
            } else {
                throw AmazonServiceExceptionTransformer.transformKeyspacesStreamsToKinesisGetShardIterator(e, skipRecordsBehavior);
            }
        } catch (AwsServiceException e) {
            throw AmazonServiceExceptionTransformer.transformKeyspacesStreamsToKinesisGetShardIterator(e, skipRecordsBehavior);
        }
        return GetShardIteratorResponse.builder()
                .shardIterator(result.shardIterator())
                .build();
    }

    @Override
    public KinesisServiceClientConfiguration serviceClientConfiguration() {
        return KinesisServiceClientConfiguration.builder()
                .region(this.region)
                .build();
    }

    /**
     * @param listStreamsRequest Container for the necessary parameters to execute the ListStreams service method on Keyspaces Streams.
     * @return The response from the ListStreams service method, adapted for use with the AmazonKinesis model.
     */
    @Override
    public CompletableFuture<ListStreamsResponse> listStreams(
            ListStreamsRequest listStreamsRequest
    ) throws AwsServiceException, SdkClientException {
        return CompletableFuture.supplyAsync(() -> {
            software.amazon.awssdk.services.keyspacesstreams.model.ListStreamsRequest keyspacesListStreamsRequest =
                    KeyspacesStreamsRequestsBuilder.listStreamsRequestBuilder()
                            .maxResults(listStreamsRequest.limit())
                            .nextToken(listStreamsRequest.nextToken())
                            .build();
            try {
                software.amazon.awssdk.services.keyspacesstreams.model.ListStreamsResponse listStreamsResponse =
                        internalClient.listStreams(keyspacesListStreamsRequest);
                return ListStreamsResponse.builder()
                        .streamNames(listStreamsResponse.streams().stream()
                                .map(software.amazon.awssdk.services.keyspacesstreams.model.Stream::streamArn)
                                .collect(Collectors.toList()))
                        .hasMoreStreams(listStreamsResponse.nextToken() != null)
                        .build();
            } catch (AwsServiceException e) {
                throw AmazonServiceExceptionTransformer.transformKeyspacesStreamsToKinesisListStreams(e, skipRecordsBehavior);
            }
        });
    }

    /**
     * @param getRecordsRequest Container for the necessary parameters to execute the GetRecords service method on Keyspaces Streams.
     * @return The response from the GetRecords service method, adapted for use with the AmazonKinesis model.
     */
    @Override
    public CompletableFuture<GetRecordsResponse> getRecords(
            GetRecordsRequest getRecordsRequest
    ) throws AwsServiceException, SdkClientException {
        throw new UnsupportedOperationException("Keyspaces Adapter does not implement kinesis getrecords. See getKeyspacesStreamsRecords function");
    }

    public CompletableFuture<GetRecordsResponseAdapter> getKeyspacesStreamsRecords(
            software.amazon.awssdk.services.keyspacesstreams.model.GetRecordsRequest keyspacesGetRecordsRequest
    ) throws AwsServiceException, SdkClientException {
        return CompletableFuture.supplyAsync(() -> {
            software.amazon.awssdk.services.keyspacesstreams.model.GetRecordsResponse result;
            try {
                result = internalClient.getRecords(keyspacesGetRecordsRequest);
            } catch (AwsServiceException e) {
                throw AmazonServiceExceptionTransformer.transformKeyspacesStreamsToKinesisGetRecords(e, skipRecordsBehavior);
            }
            return new KeyspacesStreamsGetRecordsResponseAdapter(result);
        });
    }

    /**
     * Sets a value of {@link SkipRecordsBehavior} to decide how the application handles the case when records are lost.
     * Default = {@link SkipRecordsBehavior#SKIP_RECORDS_TO_TRIM_HORIZON}
     *
     * @param skipRecordsBehavior A {@link SkipRecordsBehavior} for the adapter
     */
    public void setSkipRecordsBehavior(SkipRecordsBehavior skipRecordsBehavior) {
        if (skipRecordsBehavior == null) {
            throw new NullPointerException("skipRecordsBehavior cannot be null");
        }
        this.skipRecordsBehavior = skipRecordsBehavior;
    }

    private software.amazon.awssdk.services.keyspacesstreams.model.GetStreamResponse getStreamWithRetries(GetStreamRequest getStreamRequest) {
        software.amazon.awssdk.services.keyspacesstreams.model.GetStreamResponse getStreamResponse = null;
        int remainingRetries = MAX_GET_STREAM_RETRY_ATTEMPTS;
        AwsServiceException lastException = null;
        while (getStreamResponse == null) {
            try {
                getStreamResponse = internalClient.getStream(getStreamRequest);
            } catch (AwsServiceException e) {
                if (e instanceof ThrottlingException) {
                    log.debug("Got LimitExceededException from GetStream, retrying {} times", remainingRetries);
                    sleeper.sleep(GET_STREAM_CALLS_DELAY.toMillis());
                    lastException = e;
                }
                if (e instanceof ResourceNotFoundException) {
                    throw e;
                }
            }
            remainingRetries--;
            if (remainingRetries == 0 && getStreamResponse == null) {
                if (lastException != null) {
                    throw lastException;

                }
                throw new IllegalStateException("Received null from GetStream call.");

            }

        }
        return getStreamResponse;
    }
}