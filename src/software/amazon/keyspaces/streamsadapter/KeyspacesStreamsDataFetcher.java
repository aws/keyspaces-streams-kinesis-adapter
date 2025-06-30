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
import software.amazon.keyspaces.streamsadapter.util.KinesisMapperUtil;
import com.google.common.collect.Iterables;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.core.exception.SdkException;

import software.amazon.awssdk.services.keyspacesstreams.model.GetRecordsRequest;
import software.amazon.awssdk.services.keyspacesstreams.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.KinesisException;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.metrics.MetricsScope;
import software.amazon.kinesis.metrics.MetricsUtil;
import software.amazon.kinesis.retrieval.AWSExceptionManager;
import software.amazon.kinesis.retrieval.DataFetcherProviderConfig;
import software.amazon.kinesis.retrieval.DataFetcherResult;
import software.amazon.kinesis.retrieval.GetRecordsResponseAdapter;
import software.amazon.kinesis.retrieval.RetryableRetrievalException;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;
import software.amazon.kinesis.retrieval.polling.DataFetcher;

import java.time.Duration;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static software.amazon.keyspaces.streamsadapter.util.KinesisMapperUtil.createKeyspacesStreamsArnFromKinesisStreamName;

@Slf4j
public class KeyspacesStreamsDataFetcher implements DataFetcher {
    private static final String METRICS_PREFIX = "KeyspacesStreamsDataFetcher";
    private static final String OPERATION = "ProcessTask";

    @NonNull
    private final AmazonKeyspacesStreamsAdapterClient amazonKeyspacesStreamsAdapterClient;

    @NonNull
    @Getter
    private final StreamIdentifier streamIdentifier;

    @NonNull
    private final String shardId;
    private final int maxRecords;

    @NonNull
    private final MetricsFactory metricsFactory;
    private final String streamAndShardId;

    @Getter(AccessLevel.PACKAGE)
    private String nextIterator;

    @Getter
    private boolean isShardEndReached;

    @Getter
    private boolean isInitialized;

    @Getter
    private String lastKnownSequenceNumber;

    final Duration maxFutureWait;

    private InitialPositionInStreamExtended initialPositionInStream;

    private static final AWSExceptionManager AWS_EXCEPTION_MANAGER = createExceptionManager();
    private static AWSExceptionManager createExceptionManager() {
        final AWSExceptionManager exceptionManager = new AWSExceptionManager();
        exceptionManager.add(ResourceNotFoundException.class, t -> t);
        exceptionManager.add(KinesisException.class, t -> t);
        exceptionManager.add(SdkException.class, t -> t);
        return exceptionManager;
    }

    private static final int DEFAULT_MAX_RECORDS = 1000;

    final DataFetcherResult TERMINAL_RESULT = new DataFetcherResult() {
        @Override
        public GetRecordsResponseAdapter getResultAdapter() {
            return new KeyspacesStreamsGetRecordsResponseAdapter(
                    GetRecordsResponse.builder()
                            .changeRecords(Collections.emptyList())
                            .nextShardIterator(null)
                            .build()
            );
        }

        @Override
        public software.amazon.awssdk.services.kinesis.model.GetRecordsResponse getResult() {
            throw new UnsupportedOperationException("getResult not implemented for KeyspacesStreamsDataFetcher");
        }

        @Override
        public GetRecordsResponseAdapter acceptAdapter() {
            nextIterator = null;
            isShardEndReached = true;
            return getResultAdapter();
        }

        @Override
        public software.amazon.awssdk.services.kinesis.model.GetRecordsResponse accept() {
            throw new UnsupportedOperationException("accept not implemented for KeyspacesStreamsDataFetcher");
        }

        @Override
        public boolean isShardEnd() {
            return true;
        }
    };

    public KeyspacesStreamsDataFetcher(
            @NotNull AmazonKeyspacesStreamsAdapterClient amazonKeyspacesStreamsAdapterClient,
            DataFetcherProviderConfig keyspacesStreamsDataFetcherProviderConfig
    ) {
        this.amazonKeyspacesStreamsAdapterClient = amazonKeyspacesStreamsAdapterClient;
        this.maxRecords = Math.min(keyspacesStreamsDataFetcherProviderConfig.getMaxRecords(), DEFAULT_MAX_RECORDS);
        this.metricsFactory = keyspacesStreamsDataFetcherProviderConfig.getMetricsFactory();
        this.streamIdentifier = keyspacesStreamsDataFetcherProviderConfig.getStreamIdentifier();
        this.shardId = keyspacesStreamsDataFetcherProviderConfig.getShardId();
        this.streamAndShardId = String.format("%s:%s",
                KinesisMapperUtil.createKeyspacesStreamsArnFromKinesisStreamName(streamIdentifier.streamName()),
                shardId);
        this.maxFutureWait = keyspacesStreamsDataFetcherProviderConfig.getKinesisRequestTimeout();
    }

    @Override
    public DataFetcherResult getRecords() {
        if (!isInitialized) {
            throw new IllegalStateException("KeyspacesStreamsDataFetcher.records called before initialization.");
        }

        if (nextIterator != null) {
            try {
                return new AdvancingResult(keyspacesGetRecords(nextIterator));
            } catch (ResourceNotFoundException e) {
                log.info("Caught ResourceNotFoundException when fetching records for shard {}", streamAndShardId);
                return TERMINAL_RESULT;
            }
        } else {
            return TERMINAL_RESULT;
        }
    }

    @Override
    public void initialize(String initialCheckpoint, InitialPositionInStreamExtended initialPositionInStream) {
        log.info("Initializing stream and shard: {} with: {}", streamAndShardId, initialCheckpoint);
        advanceIteratorTo(initialCheckpoint, initialPositionInStream);
        isInitialized = true;
    }

    @Override
    public void initialize(ExtendedSequenceNumber initialCheckpoint, InitialPositionInStreamExtended initialPositionInStream) {
        log.info("Initializing stream and shard: {} with: {}", streamAndShardId, initialCheckpoint.sequenceNumber());
        advanceIteratorTo(initialCheckpoint.sequenceNumber(), initialPositionInStream);
        isInitialized = true;
    }

    @Override
    public void advanceIteratorTo(String sequenceNumber, InitialPositionInStreamExtended initialPositionInStream) {
        advanceIteratorTo(sequenceNumber, initialPositionInStream, false);
    }

    private void advanceIteratorTo(String sequenceNumber, InitialPositionInStreamExtended initialPositionInStreamExtended, boolean isIteratorRestart) {
        if (sequenceNumber == null) {
            throw new IllegalArgumentException("SequenceNumber should not be null: shardId " + shardId);
        }
        GetShardIteratorRequest.Builder getShardIteratorRequestBuilder = GetShardIteratorRequest.builder()
                .streamName(createKeyspacesStreamsArnFromKinesisStreamName(streamIdentifier.streamName()))
                .shardId(shardId);

        if (Objects.equals(ExtendedSequenceNumber.LATEST.sequenceNumber(), sequenceNumber)) {
            getShardIteratorRequestBuilder.shardIteratorType(ShardIteratorType.LATEST);
        } else if (Objects.equals(ExtendedSequenceNumber.TRIM_HORIZON.sequenceNumber(), sequenceNumber)) {
            getShardIteratorRequestBuilder.shardIteratorType(ShardIteratorType.TRIM_HORIZON);
        } else if (Objects.equals(ExtendedSequenceNumber.SHARD_END.sequenceNumber(), sequenceNumber)) {
            nextIterator = null;
            isShardEndReached = true;
            this.lastKnownSequenceNumber = sequenceNumber;
            this.initialPositionInStream = initialPositionInStreamExtended;
            return;
        } else {
            getShardIteratorRequestBuilder.shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER);
            getShardIteratorRequestBuilder.startingSequenceNumber(sequenceNumber);
        }
        GetShardIteratorRequest request = getShardIteratorRequestBuilder.build();
        log.debug("[GetShardIterator] Request has parameters {}", request);

        final MetricsScope metricsScope = MetricsUtil.createMetricsWithOperation(metricsFactory, OPERATION);
        MetricsUtil.addStreamId(metricsScope, streamIdentifier);
        MetricsUtil.addShardId(metricsScope, shardId);
        boolean success = false;
        long startTime = System.currentTimeMillis();

        try {
            try {
                nextIterator = getNextIterator(request);
                success = true;
            } catch (ExecutionException e) {
                throw AWS_EXCEPTION_MANAGER.apply(e.getCause());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (TimeoutException e) {
                throw new RetryableRetrievalException(e.getMessage(), e);
            }
        } catch (ResourceNotFoundException e) {
            log.info("Caught ResourceNotFoundException when getting an iterator for shard {}", streamAndShardId, e);
            nextIterator = null;
        } finally {
            MetricsUtil.addSuccessAndLatency(
                    metricsScope,
                    String.format("%s.%s", METRICS_PREFIX, "getShardIterator"),
                    success,
                    startTime,
                    MetricsLevel.DETAILED);
            MetricsUtil.endScope(metricsScope);
        }

        if (nextIterator == null) {
            isShardEndReached = true;
        }
        this.lastKnownSequenceNumber = sequenceNumber;
        this.initialPositionInStream = initialPositionInStreamExtended;
    }

    @Override
    public void restartIterator() {
        if (StringUtils.isEmpty(lastKnownSequenceNumber) || initialPositionInStream == null) {
            throw new IllegalArgumentException("Make sure to initialize the KeyspacesStreamsDataFetcher before restarting the iterator.");
        }
        log.debug(
                "Restarting iterator for sequence number {} on shard id {}", lastKnownSequenceNumber, streamAndShardId);
        advanceIteratorTo(lastKnownSequenceNumber, initialPositionInStream, true);
    }

    @Override
    public void resetIterator(String shardIterator, String sequenceNumber, InitialPositionInStreamExtended initialPositionInStream) {
        this.nextIterator = shardIterator;
        this.lastKnownSequenceNumber = sequenceNumber;
        this.initialPositionInStream = initialPositionInStream;
    }

    @Override
    public software.amazon.awssdk.services.kinesis.model.GetRecordsResponse
    getGetRecordsResponse(software.amazon.awssdk.services.kinesis.model.GetRecordsRequest request) throws Exception {
        throw new UnsupportedOperationException("getGetRecordsResponse is " +
                "not implemented for KeyspacesStreamsDataFetcher");
    }

    @Override
    public software.amazon.awssdk.services.kinesis.model.GetRecordsRequest getGetRecordsRequest(String nextIterator) {
        throw new UnsupportedOperationException("getGetRecordsRequest is " +
                "not implemented for KeyspacesStreamsDataFetcher");
    }

    public GetRecordsResponseAdapter getGetRecordsResponse(GetRecordsRequest request) throws ExecutionException, InterruptedException, TimeoutException {
        return amazonKeyspacesStreamsAdapterClient.getKeyspacesStreamsRecords(request).get();
    }

    public GetRecordsRequest keyspacesGetRecordsRequest(String nextIterator) {
        return KeyspacesStreamsRequestsBuilder.getRecordsRequestBuilder()
                .shardIterator(nextIterator)
                .maxResults(maxRecords)
                .build();
    }

    public String getNextIterator(GetShardIteratorRequest request) throws ExecutionException, InterruptedException, TimeoutException {
        final GetShardIteratorResponse result = amazonKeyspacesStreamsAdapterClient.getShardIterator(request).get();
        return result.shardIterator();
    }

    @Override
    public software.amazon.awssdk.services.kinesis.model.GetRecordsResponse getRecords(@NonNull String nextIterator) {
        throw new UnsupportedOperationException("getRecords is not implemented for KeyspacesStreamsDataFetcher");
    }

    public GetRecordsResponseAdapter keyspacesGetRecords(@NonNull String nextIterator) {
        GetRecordsRequest getRecordsRequest = keyspacesGetRecordsRequest(nextIterator);
        final MetricsScope metricsScope = MetricsUtil.createMetricsWithOperation(metricsFactory, OPERATION);
        MetricsUtil.addStreamId(metricsScope, streamIdentifier);
        MetricsUtil.addShardId(metricsScope, shardId);
        boolean success = false;
        long startTime = System.currentTimeMillis();
        try {
            final GetRecordsResponseAdapter response = getGetRecordsResponse(getRecordsRequest);
            success = true;
            return response;
        } catch (ExecutionException e) {
            throw AWS_EXCEPTION_MANAGER.apply(e.getCause());
        } catch (InterruptedException e) {
            log.debug("{} : Interrupt called on method, shutdown initiated", streamAndShardId);
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            throw new RetryableRetrievalException(e.getMessage(), e);
        } finally {
            MetricsUtil.addSuccessAndLatency(
                    metricsScope,
                    String.format("%s.%s", METRICS_PREFIX, "getRecords"),
                    success,
                    startTime,
                    MetricsLevel.DETAILED);
            MetricsUtil.endScope(metricsScope);
        }
    }

    @Data
    class AdvancingResult implements DataFetcherResult {
        final GetRecordsResponseAdapter result;

        @Override
        public GetRecordsResponseAdapter getResultAdapter() {
            return result;
        }

        @Override
        public software.amazon.awssdk.services.kinesis.model.GetRecordsResponse getResult() {
            throw new UnsupportedOperationException("AdvancingResult.getResult is " +
                    "not implemented for KeyspacesStreamsDataFetcher");
        }

        @Override
        public GetRecordsResponseAdapter acceptAdapter() {
            nextIterator = result.nextShardIterator();
            if (result.records() != null && !result.records().isEmpty()) {
                lastKnownSequenceNumber = Iterables.getLast(result.records()).sequenceNumber();
            }
            if (nextIterator == null) {
                isShardEndReached = true;
            }
            return getResultAdapter();
        }

        @Override
        public software.amazon.awssdk.services.kinesis.model.GetRecordsResponse accept() {
            throw new UnsupportedOperationException("AdvancingResult.accept is " +
                    "not implemented for KeyspacesStreamsDataFetcher");
        }

        @Override
        public boolean isShardEnd() {
            return isShardEndReached;
        }
    }
}