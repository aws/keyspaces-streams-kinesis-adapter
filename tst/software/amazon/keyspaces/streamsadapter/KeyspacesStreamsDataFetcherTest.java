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
import software.amazon.keyspaces.streamsadapter.util.KinesisMapperUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.keyspacesstreams.model.GetRecordsRequest;
import software.amazon.awssdk.services.keyspacesstreams.model.GetRecordsResponse;
import software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesRow;
import software.amazon.awssdk.services.keyspacesstreams.model.Record;
import software.amazon.awssdk.services.keyspacesstreams.model.StreamStatus;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StreamDescription;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.NullMetricsFactory;
import software.amazon.kinesis.retrieval.DataFetcherProviderConfig;
import software.amazon.kinesis.retrieval.DataFetcherResult;
import software.amazon.kinesis.retrieval.GetRecordsResponseAdapter;
import software.amazon.kinesis.retrieval.KinesisDataFetcherProviderConfig;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class KeyspacesStreamsDataFetcherTest {

    private static final String STREAM_ARN = "arn:aws:cassandra:us-west-2:111122223333:/keyspace/test_keyspace/table/TestTable/stream/2024-02-03T00:00:00.000";
    private static final String STREAM_NAME = KinesisMapperUtil.createKinesisStreamIdentifierFromKeyspacesStreamsArn(STREAM_ARN, false);
    private static final String SHARD_ID = "shard-000001";
    private static final String SEQUENCE_NUMBER = "123";
    private static final String ITERATOR = "iterator-123";
    private static final int MAX_RECORDS = 100;

    private AmazonKeyspacesStreamsAdapterClient amazonKeyspacesStreamsAdapterClient;

    private KeyspacesStreamsDataFetcher keyspacesStreamsDataFetcher;
    private StreamIdentifier streamIdentifier;
    private DataFetcherProviderConfig dataFetcherProviderConfig;
    private MetricsFactory metricsFactory;

    @BeforeEach
    void setup() {
        amazonKeyspacesStreamsAdapterClient = Mockito.mock(AmazonKeyspacesStreamsAdapterClient.class);
        streamIdentifier = StreamIdentifier.singleStreamInstance(STREAM_NAME);
        metricsFactory = new NullMetricsFactory();
        dataFetcherProviderConfig = new KinesisDataFetcherProviderConfig(streamIdentifier, SHARD_ID, metricsFactory, MAX_RECORDS, Duration.ofMillis(30000L));
        keyspacesStreamsDataFetcher = new KeyspacesStreamsDataFetcher(amazonKeyspacesStreamsAdapterClient, dataFetcherProviderConfig);
    }

    @Test
    void testInitializeWithSequenceNumber() {
        mockGetShardIterator(SEQUENCE_NUMBER, ShardIteratorType.AFTER_SEQUENCE_NUMBER, ITERATOR);

        keyspacesStreamsDataFetcher.initialize(
                SEQUENCE_NUMBER,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST)
        );
        assertTrue(keyspacesStreamsDataFetcher.isInitialized());
        assertEquals(SEQUENCE_NUMBER, keyspacesStreamsDataFetcher.getLastKnownSequenceNumber());
        assertEquals(ITERATOR, keyspacesStreamsDataFetcher.getNextIterator());
    }

    @Test
    void testInitializeWithTrimHorizon() {
        mockGetShardIterator(null, ShardIteratorType.TRIM_HORIZON, ITERATOR);

        keyspacesStreamsDataFetcher.initialize(
                ExtendedSequenceNumber.TRIM_HORIZON,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON)
        );

        assertTrue(keyspacesStreamsDataFetcher.isInitialized());
        assertEquals(keyspacesStreamsDataFetcher.getLastKnownSequenceNumber(), InitialPositionInStream.TRIM_HORIZON.toString());
        assertEquals(ITERATOR, keyspacesStreamsDataFetcher.getNextIterator());
    }

    @Test
    void testInitializeWithLatest() {
        mockGetShardIterator(null, ShardIteratorType.LATEST, ITERATOR);

        keyspacesStreamsDataFetcher.initialize(
                ExtendedSequenceNumber.LATEST,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST)
        );

        assertTrue(keyspacesStreamsDataFetcher.isInitialized());
        assertEquals(keyspacesStreamsDataFetcher.getLastKnownSequenceNumber(), InitialPositionInStream.LATEST.toString());
        assertEquals(ITERATOR, keyspacesStreamsDataFetcher.getNextIterator());
    }

    @Test
    void testKeyspacesGetRecordsWithoutInitialization() {
        IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> keyspacesStreamsDataFetcher.getRecords()
        );
        assertEquals("KeyspacesStreamsDataFetcher.records called before initialization.", exception.getMessage());
    }

    @Test
    void testKeyspacesGetRecordsSuccess() {
        // Setup
        mockGetShardIterator(SEQUENCE_NUMBER, ShardIteratorType.AFTER_SEQUENCE_NUMBER, ITERATOR);
        keyspacesStreamsDataFetcher.initialize(SEQUENCE_NUMBER, InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST));

        Record record1 = createRecord("1");
        Record record2 = createRecord("2");
        GetRecordsResponseAdapter response = new KeyspacesStreamsGetRecordsResponseAdapter(
                GetRecordsResponse.builder()
                        .changeRecords(Arrays.asList(record1, record2))
                        .nextShardIterator("next-iterator")
                        .build());

        when(amazonKeyspacesStreamsAdapterClient.getKeyspacesStreamsRecords(any(GetRecordsRequest.class)))
                .thenReturn(CompletableFuture.supplyAsync(() -> response));

        // Execute
        DataFetcherResult result = keyspacesStreamsDataFetcher.getRecords();

        // Verify
        assertNotNull(result);
        assertEquals(2, result.getResultAdapter().records().size());
        assertEquals("next-iterator", result.getResultAdapter().nextShardIterator());
        assertFalse(result.isShardEnd());
    }

    @Test
    void testKeyspacesGetRecordsWithResourceNotFound() {
        // Setup
        mockGetShardIterator(SEQUENCE_NUMBER, ShardIteratorType.AFTER_SEQUENCE_NUMBER, ITERATOR);
        keyspacesStreamsDataFetcher.initialize(SEQUENCE_NUMBER, InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST));

        when(amazonKeyspacesStreamsAdapterClient.getKeyspacesStreamsRecords(any(GetRecordsRequest.class)))
                .thenThrow(ResourceNotFoundException.builder().build());

        // Execute
        DataFetcherResult result = keyspacesStreamsDataFetcher.getRecords();

        // Verify
        assertNotNull(result);
        assertTrue(result.getResultAdapter().records().isEmpty());
        assertNull(result.getResultAdapter().nextShardIterator());
    }

    @Test
    void testShardEndReached() {
        // Setup
        mockGetShardIterator(SEQUENCE_NUMBER, ShardIteratorType.AFTER_SEQUENCE_NUMBER, ITERATOR);
        keyspacesStreamsDataFetcher.initialize(SEQUENCE_NUMBER, InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST));

        GetRecordsResponse response = GetRecordsResponse.builder()
                .changeRecords(Collections.emptyList())
                .nextShardIterator(null)  // Null iterator indicates shard end
                .build();

        when(amazonKeyspacesStreamsAdapterClient.getKeyspacesStreamsRecords(any(GetRecordsRequest.class)))
                .thenReturn(CompletableFuture.supplyAsync(() -> new KeyspacesStreamsGetRecordsResponseAdapter(response)));

        // Execute
        DataFetcherResult result = keyspacesStreamsDataFetcher.getRecords();
        result.acceptAdapter();

        // Verify
        assertTrue(keyspacesStreamsDataFetcher.isShardEndReached());
    }

    @Test
    void testRestartIterator() {
        // Setup
        mockGetShardIterator(SEQUENCE_NUMBER, ShardIteratorType.AFTER_SEQUENCE_NUMBER, ITERATOR);
        keyspacesStreamsDataFetcher.initialize(SEQUENCE_NUMBER, InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST));

        mockGetShardIterator(SEQUENCE_NUMBER, ShardIteratorType.AFTER_SEQUENCE_NUMBER, "new-iterator");

        // Execute
        keyspacesStreamsDataFetcher.restartIterator();

        // Verify
        assertEquals("new-iterator", keyspacesStreamsDataFetcher.getNextIterator());
    }

    @Test
    void testRestartIteratorWithoutInitialization() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> keyspacesStreamsDataFetcher.restartIterator()
        );
        assertEquals("Make sure to initialize the KeyspacesStreamsDataFetcher before restarting the iterator.",
                exception.getMessage());
    }

    @Test
    void testGetKeyspacesGetRecordsResponseWithShardEndAndDisabledStream() throws Exception {
        // Setup
        GetRecordsRequest request = GetRecordsRequest.builder()
                .shardIterator("some-iterator")
                .maxResults(100)
                .build();

        // Create GetRecordsResponse with null nextShardIterator
        GetRecordsResponse recordsResponse = GetRecordsResponse.builder()
                .changeRecords(Collections.emptyList())
                .nextShardIterator(null)
                .build();

        // Create DescribeStream response for disabled stream
        DescribeStreamResponse describeStreamResponse = DescribeStreamResponse.builder()
                .streamDescription(StreamDescription.builder()
                        .streamName(STREAM_NAME)
                        .streamStatus(StreamStatus.DISABLED.toString())
                        .shards(Collections.emptyList())
                        .hasMoreShards(false)
                        .build())
                .build();

        // Mock responses
        when(amazonKeyspacesStreamsAdapterClient.getKeyspacesStreamsRecords(any(GetRecordsRequest.class)))
                .thenReturn(CompletableFuture.supplyAsync(() -> new KeyspacesStreamsGetRecordsResponseAdapter(recordsResponse)));

        // Execute
        GetRecordsResponseAdapter result = keyspacesStreamsDataFetcher.getGetRecordsResponse(request);

        // Verify
        assertNotNull(result);
        assertTrue(result.records().isEmpty());
        assertNull(result.nextShardIterator());
        assertTrue(result.childShards().isEmpty());
    }

    @Test
    void testGetKeyspacesGetRecordsResponseWithActiveIterator() throws Exception {
        // Setup
        GetRecordsRequest request = GetRecordsRequest.builder()
                .shardIterator("active-iterator")
                .maxResults(100)
                .build();

        // Create GetRecordsResponse with active iterator
        GetRecordsResponse recordsResponse = GetRecordsResponse.builder()
                .changeRecords(Collections.emptyList())
                .nextShardIterator("next-iterator")  // Active iterator
                .build();

        when(amazonKeyspacesStreamsAdapterClient.getKeyspacesStreamsRecords(any(GetRecordsRequest.class)))
                .thenReturn(CompletableFuture.supplyAsync(() -> new KeyspacesStreamsGetRecordsResponseAdapter(recordsResponse)));

        // Execute
        GetRecordsResponseAdapter result = keyspacesStreamsDataFetcher.getGetRecordsResponse(request);

        // Verify
        assertNotNull(result);
        assertTrue(result.records().isEmpty());
        assertEquals("next-iterator", result.nextShardIterator());
    }

    @Test
    void testInitializeWithShardEnd() {
        // Initialize with SHARD_END
        keyspacesStreamsDataFetcher.initialize(
                ExtendedSequenceNumber.SHARD_END,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST)
        );

        // Verify
        assertTrue(keyspacesStreamsDataFetcher.isInitialized());
        assertTrue(keyspacesStreamsDataFetcher.isShardEndReached());
        assertNull(keyspacesStreamsDataFetcher.getNextIterator());
        assertEquals(ExtendedSequenceNumber.SHARD_END.sequenceNumber(), keyspacesStreamsDataFetcher.getLastKnownSequenceNumber());
    }

    @Test
    void testResetIterator() {
        // Setup
        String shardIterator = "test-iterator";
        String sequenceNumber = "test-sequence";
        InitialPositionInStreamExtended initialPosition = InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST);

        // Execute
        keyspacesStreamsDataFetcher.resetIterator(shardIterator, sequenceNumber, initialPosition);

        // Verify
        assertEquals(shardIterator, keyspacesStreamsDataFetcher.getNextIterator());
        assertEquals(sequenceNumber, keyspacesStreamsDataFetcher.getLastKnownSequenceNumber());
    }

    private void mockGetShardIterator(String sequenceNumber, ShardIteratorType iteratorType, String iterator) {
        when(amazonKeyspacesStreamsAdapterClient.getShardIterator(
                GetShardIteratorRequest.builder()
                        .streamName(KinesisMapperUtil.createKeyspacesStreamsArnFromKinesisStreamName(STREAM_NAME))
                        .shardId(SHARD_ID)
                        .startingSequenceNumber(sequenceNumber)
                        .shardIteratorType(iteratorType)
                        .build())
        )
                .thenReturn(
                        CompletableFuture.supplyAsync(() ->
                                GetShardIteratorResponse.builder()
                                        .shardIterator(iterator)
                                        .build()
                        )
                );
    }

    private Record createRecord(String sequenceNumber) {
        return Record.builder()
                .sequenceNumber(sequenceNumber)
                .createdAt(Instant.now())
                .eventVersion("1.0")
                .origin("keyspaces")
                .newImage(KeyspacesRow.builder().build())
                .build();
    }
}