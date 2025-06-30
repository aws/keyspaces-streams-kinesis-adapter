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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.keyspacesstreams.KeyspacesStreamsClient;
import software.amazon.awssdk.services.keyspacesstreams.model.GetRecordsRequest;
import software.amazon.awssdk.services.keyspacesstreams.model.GetRecordsResponse;
import software.amazon.awssdk.services.keyspacesstreams.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.keyspacesstreams.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.keyspacesstreams.model.GetStreamRequest;
import software.amazon.awssdk.services.keyspacesstreams.model.GetStreamResponse;
import software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesRow;
import software.amazon.awssdk.services.keyspacesstreams.model.ListStreamsRequest;
import software.amazon.awssdk.services.keyspacesstreams.model.ListStreamsResponse;
import software.amazon.awssdk.services.keyspacesstreams.model.Record;
import software.amazon.awssdk.services.keyspacesstreams.model.Shard;
import software.amazon.awssdk.services.keyspacesstreams.model.SequenceNumberRange;
import software.amazon.awssdk.services.keyspacesstreams.model.Stream;
import software.amazon.awssdk.services.keyspacesstreams.model.StreamStatus;
import software.amazon.awssdk.services.keyspacesstreams.model.ValidationException;
import software.amazon.awssdk.services.keyspacesstreams.model.ValidationExceptionType;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.kinesis.retrieval.GetRecordsResponseAdapter;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AmazonKeyspacesStreamsAdapterClientTest {

    @Mock
    private AwsCredentialsProvider awsCredentialsProvider;
    @Mock
    private KeyspacesStreamsClient keyspacesStreamsClient;

    private Region region = Region.US_WEST_2;

    private AmazonKeyspacesStreamsAdapterClient adapterClient;

    private static final String STREAM_ARN = "arn:aws:cassandra:us-west-2:123456789012:/keyspace/test_keyspace/table/TestTable/stream/2024-02-03T00:00:00.000";
    private static final String SHARD_ID = "shardId-00000000000000000000-00000000";
    private static final String SEQUENCE_NUMBER = "100";
    private static final String ITERATOR = "iterator-value";

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);
        adapterClient = new AmazonKeyspacesStreamsAdapterClient(keyspacesStreamsClient);
    }

    @Test
    void testDescribeStreamUnsupported() {
        // Create a Kinesis DescribeStreamRequest
        software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest kinesisRequest =
                software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest.builder()
                        .streamName(STREAM_ARN)
                        .build();

        // Verify that the describeStream method throws UnsupportedOperationException
        assertThrows(UnsupportedOperationException.class,
                () -> adapterClient.describeStream(kinesisRequest).join());
    }

    @Test
    void testGetStream() {
        // Setup
        GetStreamRequest keyspacesRequest = GetStreamRequest.builder()
                .streamArn(STREAM_ARN)
                .build();

        GetStreamResponse keyspacesResponse = GetStreamResponse.builder()
                .streamArn(STREAM_ARN)
                .streamStatus(StreamStatus.ENABLED)
                .keyspaceName("test_keyspace")
                .tableName("TestTable")
                .streamLabel("2024-02-03T00:00:00.000")
                .shards(Collections.singletonList(
                        Shard.builder()
                                .shardId(SHARD_ID)
                                .parentShardIds(Collections.singletonList("parent-1"))
                                .sequenceNumberRange(SequenceNumberRange.builder()
                                        .startingSequenceNumber("100")
                                        .endingSequenceNumber("200")
                                        .build())
                                .build()
                ))
                .build();

        when(keyspacesStreamsClient.getStream(any(GetStreamRequest.class)))
                .thenReturn(keyspacesResponse);

        // Execute
        GetStreamResponse response = adapterClient.getStream(keyspacesRequest).join();

        // Verify
        assertNotNull(response);
        assertEquals(STREAM_ARN, response.streamArn());
        assertEquals(StreamStatus.ENABLED, response.streamStatus());
        assertEquals("test_keyspace", response.keyspaceName());
        assertEquals("TestTable", response.tableName());
        assertEquals("2024-02-03T00:00:00.000", response.streamLabel());
        assertEquals(1, response.shards().size());
        assertEquals(SHARD_ID, response.shards().get(0).shardId());

        verify(keyspacesStreamsClient).getStream(any(GetStreamRequest.class));
    }

    @Test
    void testGetShardIterator() {
        // Setup Keyspaces response
        GetShardIteratorResponse keyspacesResponse = GetShardIteratorResponse.builder()
                .shardIterator(ITERATOR)
                .build();

        when(keyspacesStreamsClient.getShardIterator(any(GetShardIteratorRequest.class)))
                .thenReturn(keyspacesResponse);

        // Execute
        software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest kinesisRequest =
                software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest.builder()
                        .streamName(STREAM_ARN)
                        .shardId(SHARD_ID)
                        .shardIteratorType(ShardIteratorType.AT_SEQUENCE_NUMBER)
                        .startingSequenceNumber(SEQUENCE_NUMBER)
                        .build();

        software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse response =
                adapterClient.getShardIterator(kinesisRequest).join();

        // Verify
        assertNotNull(response);
        assertEquals(ITERATOR, response.shardIterator());
    }

    @Test
    void testGetShardIteratorWithTrimmedDataAndSkip() {
        // Setup initial failure with ValidationException for trimmed data
        ValidationException trimmedDataException = ValidationException.builder()
                .errorCode(ValidationExceptionType.TRIMMED_DATA_ACCESS)
                .build();

        when(keyspacesStreamsClient.getShardIterator(any(GetShardIteratorRequest.class)))
                .thenThrow(trimmedDataException)
                .thenReturn(GetShardIteratorResponse.builder().shardIterator(ITERATOR).build());

        adapterClient.setSkipRecordsBehavior(
                AmazonKeyspacesStreamsAdapterClient.SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON);

        // Execute
        software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest kinesisRequest =
                software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest.builder()
                        .streamName(STREAM_ARN)
                        .shardId(SHARD_ID)
                        .shardIteratorType(ShardIteratorType.AT_SEQUENCE_NUMBER)
                        .startingSequenceNumber(SEQUENCE_NUMBER)
                        .build();

        software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse response =
                adapterClient.getShardIterator(kinesisRequest).join();

        // Verify
        assertNotNull(response);
        assertEquals(ITERATOR, response.shardIterator());
    }

    @Test
    void testListStreams() {
        // Setup Keyspaces response
        ListStreamsResponse keyspacesResponse = ListStreamsResponse.builder()
                .streams(Arrays.asList(
                        Stream.builder()
                                .streamArn(STREAM_ARN)
                                .keyspaceName("test_keyspace")
                                .tableName("TestTable")
                                .build()))
                .nextToken("next-token")
                .build();

        when(keyspacesStreamsClient.listStreams(any(ListStreamsRequest.class)))
                .thenReturn(keyspacesResponse);

        // Execute
        software.amazon.awssdk.services.kinesis.model.ListStreamsRequest kinesisRequest =
                software.amazon.awssdk.services.kinesis.model.ListStreamsRequest.builder()
                        .limit(100)
                        .exclusiveStartStreamName(null)
                        .build();

        software.amazon.awssdk.services.kinesis.model.ListStreamsResponse response =
                adapterClient.listStreams(kinesisRequest).join();

        // Verify
        assertNotNull(response);
        assertEquals(1, response.streamNames().size());
        assertEquals(STREAM_ARN, response.streamNames().get(0));
        assertTrue(response.hasMoreStreams());
    }

    @Test
    void testGetRecords() {
        // Setup Keyspaces response
        GetRecordsResponse keyspacesResponse = GetRecordsResponse.builder()
                .changeRecords(Collections.singletonList(
                        Record.builder()
                                .sequenceNumber(SEQUENCE_NUMBER)
                                .createdAt(Instant.now())
                                .eventVersion("1.0")
                                .origin("keyspaces")
                                .newImage(KeyspacesRow.builder().build())
                                .build()))
                .nextShardIterator("next-" + ITERATOR)
                .build();

        when(keyspacesStreamsClient.getRecords(any(GetRecordsRequest.class)))
                .thenReturn(keyspacesResponse);

        // Execute
        GetRecordsRequest keyspacesRequest =
                GetRecordsRequest.builder()
                        .shardIterator(ITERATOR)
                        .maxResults(100)
                        .build();

        GetRecordsResponseAdapter response =
                adapterClient.getKeyspacesStreamsRecords(keyspacesRequest).join();

        // Verify
        assertNotNull(response);
        assertEquals(1, response.records().size());
        assertEquals("next-" + ITERATOR, response.nextShardIterator());
    }

    @Test
    void testGetRecordsWithEmptyRecords() {
        // Setup Keyspaces response with empty records
        GetRecordsResponse keyspacesResponse = GetRecordsResponse.builder()
                .changeRecords(Collections.emptyList())
                .nextShardIterator("next-" + ITERATOR)
                .build();

        when(keyspacesStreamsClient.getRecords(any(GetRecordsRequest.class)))
                .thenReturn(keyspacesResponse);

        // Execute
        GetRecordsRequest keyspacesRequest = GetRecordsRequest.builder()
                .shardIterator(ITERATOR)
                .maxResults(100)
                .build();

        GetRecordsResponseAdapter response =
                adapterClient.getKeyspacesStreamsRecords(keyspacesRequest).join();

        // Verify
        assertNotNull(response);
        assertTrue(response.records().isEmpty());
        assertEquals("next-" + ITERATOR, response.nextShardIterator());
    }

    @Test
    void testSetSkipRecordsBehavior() {
        // Test setting valid behavior
        adapterClient.setSkipRecordsBehavior(
                AmazonKeyspacesStreamsAdapterClient.SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON);
        assertEquals(
                AmazonKeyspacesStreamsAdapterClient.SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON,
                adapterClient.getSkipRecordsBehavior());

        adapterClient.setSkipRecordsBehavior(
                AmazonKeyspacesStreamsAdapterClient.SkipRecordsBehavior.KCL_RETRY);
        assertEquals(
                AmazonKeyspacesStreamsAdapterClient.SkipRecordsBehavior.KCL_RETRY,
                adapterClient.getSkipRecordsBehavior());

        // Test setting null behavior
        assertThrows(NullPointerException.class, () -> adapterClient.setSkipRecordsBehavior(null));
    }

    @Test
    void testServiceName() {
        when(keyspacesStreamsClient.serviceName()).thenReturn("keyspaces-streams");
        assertEquals("keyspaces-streams", adapterClient.serviceName());
    }

    @Test
    void testClose() {
        adapterClient.close();
        verify(keyspacesStreamsClient).close();
    }

    @Test
    void testGetRecordsUnsupported() {
        // Verify that the getRecords method throws UnsupportedOperationException
        software.amazon.awssdk.services.kinesis.model.GetRecordsRequest kinesisRequest =
                software.amazon.awssdk.services.kinesis.model.GetRecordsRequest.builder()
                        .shardIterator(ITERATOR)
                        .limit(100)
                        .build();

        assertThrows(UnsupportedOperationException.class, () -> adapterClient.getRecords(kinesisRequest));
    }
}