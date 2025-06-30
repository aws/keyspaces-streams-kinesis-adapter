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
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.exceptions.internal.KinesisClientLibIOException;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.MultiStreamLease;
import software.amazon.kinesis.metrics.MetricsScope;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import java.time.Duration;
import java.time.Instant;
import java.lang.reflect.Field;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static software.amazon.keyspaces.streamsadapter.util.TestUtils.createActiveLease;
import static software.amazon.keyspaces.streamsadapter.util.TestUtils.createCompletedLease;
import static software.amazon.keyspaces.streamsadapter.util.TestUtils.createShard;
import static software.amazon.keyspaces.streamsadapter.util.TestUtils.createTestLease;
import static software.amazon.keyspaces.streamsadapter.util.TestUtils.createTestShard;
import static software.amazon.keyspaces.streamsadapter.util.TestUtils.shardId;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KeyspacesStreamsShardSyncerTest {

    private KeyspacesStreamsShardSyncer shardSyncer;

    @Mock
    private KeyspacesStreamsShardDetector shardDetector;  // Changed to specific type

    @Mock
    private LeaseRefresher leaseRefresher;

    @Mock
    private MetricsScope metricsScope;

    private static final String STREAM_ARN = "arn:aws:cassandra:region:account:/keyspace/keyspacename/table/name/stream/timestamp";
    private static final StreamIdentifier STREAM_IDENTIFIER = StreamIdentifier.singleStreamInstance(STREAM_ARN);

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);
        shardSyncer = new KeyspacesStreamsShardSyncer();
        when(shardDetector.streamIdentifier()).thenReturn(STREAM_IDENTIFIER);
    }

    @Test
    void testCheckAndCreateLeaseForNewShardsWithEmptyShardList() throws Exception {
        // Setup
        when(shardDetector.listShards()).thenReturn(Collections.emptyList());
        // Execute
        boolean result = shardSyncer.checkAndCreateLeaseForNewShards(
                shardDetector,
                leaseRefresher,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON),
                metricsScope,
                false,
                true
        );

        // Verify
        assertTrue(result);
        verify(leaseRefresher, never()).createLeaseIfNotExists(any(Lease.class));
    }

    @Test
    void testCheckAndCreateLeaseForNewShardsWithSingleShard() throws Exception {
        // Setup
        String shardId = "shardId-000000000000";
        Shard shard = createTestShard(shardId, null, null);
        List<Shard> shards = Collections.singletonList(shard);

        // Mock both listShards and describeStream
        when(shardDetector.listShards()).thenReturn(shards);
        when(leaseRefresher.listLeases()).thenReturn(Collections.emptyList());

        // Execute
        boolean result = shardSyncer.checkAndCreateLeaseForNewShards(
                shardDetector,
                leaseRefresher,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON),
                metricsScope,
                false,
                true
        );

        // Verify
        assertTrue(result);
        verify(leaseRefresher, atLeastOnce()).createLeaseIfNotExists(any(Lease.class));
    }

    @Test
    void testDetermineNewLeasesToCreateForClosedLeafShard() {
        // Setup
        // for closed Leaf Shard Shard-Syncer should not create any leases
        String shardId = "shardId-000000000000";
        List<Shard> shards = Collections.singletonList(createShard(shardId, null, "0","1"));
        List<Lease> currentLeases = Collections.emptyList();

        // Execute
        List<Lease> newLeases = shardSyncer.determineNewLeasesToCreate(
                shards,
                currentLeases,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON),
                new HashSet<>(),
                new KeyspacesStreamsShardSyncer.MultiStreamArgs(false, null)
        );

        // Verify
        assertEquals(0, newLeases.size());
    }

    @Test
    void testMultiStreamModeWithSingleStreamHavingSingleShard() throws Exception {
        // Setup
        String streamArn = "arn:aws:cassandra:us-west-2:123456789012:/keyspace/TestKeyspace/table/TestTable/stream/2024-02-03T00:00:00.000";
        StreamIdentifier streamIdentifier = StreamIdentifier.singleStreamInstance(streamArn);
        KeyspacesStreamsShardSyncer multiStreamSyncer = new KeyspacesStreamsShardSyncer(
                true, // isMultiStreamMode
                streamArn
        );

        String shardId = "shardId-000000000000";
        Shard shard = createTestShard(shardId, null, null);
        List<Shard> shards = Collections.singletonList(shard);

        // Mock ShardDetector
        when(shardDetector.listShards()).thenReturn(shards);
        when(shardDetector.streamIdentifier()).thenReturn(streamIdentifier);

        // Mock empty existing leases
        when(leaseRefresher.listLeasesForStream(streamIdentifier)).thenReturn(Collections.emptyList());

        // Execute
        boolean result = multiStreamSyncer.checkAndCreateLeaseForNewShards(
                shardDetector,
                leaseRefresher,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON),
                metricsScope,
                false,
                true
        );

        // Verify
        assertTrue(result);

        // Capture and verify the created lease
        ArgumentCaptor<Lease> leaseCaptor = ArgumentCaptor.forClass(Lease.class);
        verify(leaseRefresher).createLeaseIfNotExists(leaseCaptor.capture());

        Lease capturedLease = leaseCaptor.getValue();
        assertTrue(capturedLease instanceof MultiStreamLease);
        MultiStreamLease multiStreamLease = (MultiStreamLease) capturedLease;
        assertEquals(streamArn, multiStreamLease.streamIdentifier());
        assertEquals(shardId, multiStreamLease.shardId());
    }

    @Test
    void testMultiStreamModeWithMultipleStreams() throws Exception {
        // Setup multiple streams
        String stream1Arn = "arn:aws:cassandra:us-west-2:123456789012:/keyspace/Keyspace1/table/Table1/stream/2024-02-03T00:00:00.000";
        String stream2Arn = "arn:aws:cassandra:us-west-2:123456789012:/keyspace/Keyspace2/table/Table2/stream/2024-02-03T00:00:00.000";
        StreamIdentifier stream1Identifier = StreamIdentifier.singleStreamInstance(stream1Arn);
        StreamIdentifier stream2Identifier = StreamIdentifier.singleStreamInstance(stream2Arn);

        KeyspacesStreamsShardSyncer multiStreamSyncer = new KeyspacesStreamsShardSyncer(
                true,
                stream1Arn,
                null // deletedStreamListProvider
        );

        // Create timestamps and shard IDs
        long baseTimestamp = System.currentTimeMillis() - Duration.ofHours(7).toMillis();
        String shard1Id = String.format("shardId-%019d-001", baseTimestamp);
        String shard2Id = String.format("shardId-%019d-002", baseTimestamp);

        // Create test shards for both streams with proper sequence numbers
        Shard shard1 = createTestShard(shard1Id, null, null, "0", null);
        Shard shard2 = createTestShard(shard2Id, null, null, "0", null);

        // Setup first stream detector
        KeyspacesStreamsShardDetector detector1 = mock(KeyspacesStreamsShardDetector.class);
        when(detector1.listShards()).thenReturn(Collections.singletonList(shard1));
        when(detector1.streamIdentifier()).thenReturn(stream1Identifier);

        // Setup second stream detector
        KeyspacesStreamsShardDetector detector2 = mock(KeyspacesStreamsShardDetector.class);
        when(detector2.listShards()).thenReturn(Collections.singletonList(shard2));
        when(detector2.streamIdentifier()).thenReturn(stream2Identifier);

        // Mock lease refresher for multiple streams
        when(leaseRefresher.listLeasesForStream(any())).thenReturn(Collections.emptyList());

        // Execute for both streams
        boolean result1 = multiStreamSyncer.checkAndCreateLeaseForNewShards(
                detector1,
                leaseRefresher,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON),
                metricsScope,
                false,
                true
        );

        boolean result2 = multiStreamSyncer.checkAndCreateLeaseForNewShards(
                detector2,
                leaseRefresher,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON),
                metricsScope,
                false,
                true
        );

        // Verify
        assertTrue(result1);
        assertTrue(result2);

        ArgumentCaptor<Lease> leaseCaptor = ArgumentCaptor.forClass(Lease.class);
        verify(leaseRefresher, times(2)).createLeaseIfNotExists(leaseCaptor.capture());

        List<Lease> capturedLeases = leaseCaptor.getAllValues();
        assertEquals(2, capturedLeases.size());

        // Verify first stream lease
        MultiStreamLease lease1 = (MultiStreamLease) capturedLeases.get(0);
        assertEquals(stream1Arn, lease1.streamIdentifier());
        assertEquals(shard1Id, lease1.shardId());

        // Verify second stream lease
        MultiStreamLease lease2 = (MultiStreamLease) capturedLeases.get(1);
        assertEquals(stream2Arn, lease2.streamIdentifier());
        assertEquals(shard2Id, lease2.shardId());
    }

    @Test
    void testCheckpointInitializationForLatestPosition() throws Exception {
        long baseTimestamp = System.currentTimeMillis() - Duration.ofHours(7).toMillis();

        String rootShardId = String.format("shardId-%019d-001", baseTimestamp);
        String childShardId = String.format("shardId-%019d-002", baseTimestamp + 1000);
        String independentShardId = String.format("shardId-%019d-003", baseTimestamp + 2000);

        Shard rootShard = createTestShard(rootShardId, null, null, "0", "100");
        Shard childShard = createTestShard(childShardId, rootShardId, null, "101", null);
        Shard independentShard = createTestShard(independentShardId, null, null, "0", null);

        List<Shard> shards = Arrays.asList(rootShard, childShard, independentShard);
        when(shardDetector.listShards()).thenReturn(shards);
        when(leaseRefresher.listLeases()).thenReturn(Collections.emptyList());

        // Test with LATEST position
        shardSyncer.checkAndCreateLeaseForNewShards(
                shardDetector,
                leaseRefresher,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST),
                metricsScope,
                false,
                true
        );

        // Capture leases created with LATEST position
        ArgumentCaptor<Lease> leaseCaptor = ArgumentCaptor.forClass(Lease.class);
        verify(leaseRefresher, times(2)).createLeaseIfNotExists(leaseCaptor.capture());

        List<Lease> capturedLeases = leaseCaptor.getAllValues();
        Map<String, Lease> leaseMap = capturedLeases.stream()
                .collect(Collectors.toMap(Lease::leaseKey, l -> l));

        assertFalse(leaseMap.containsKey(rootShardId),
                "Lease map should not contain parent lease");

        // Verify checkpoint initialization for LATEST position
        verifyLease(leaseMap.get(childShardId), childShardId,
                Collections.singletonList(rootShardId), ExtendedSequenceNumber.LATEST);

        // Independent shard (non-descendant) should get LATEST
        verifyLease(leaseMap.get(independentShardId), independentShardId,
                Collections.emptyList(), ExtendedSequenceNumber.LATEST);
    }

    @Test
    void testMultiStreamSyncerInitialization() {
        // Setup
        String streamArn = "arn:aws:cassandra:region:account:/keyspace/keyspacename/table/name/stream/timestamp";
        KeyspacesStreamsShardSyncer multiStreamSyncer = new KeyspacesStreamsShardSyncer(
                true,
                streamArn
        );

        // Use reflection to verify the initialization
        try {
            Field isMultiStreamModeField = KeyspacesStreamsShardSyncer.class.getDeclaredField("isMultiStreamMode");
            Field streamIdentifierField = KeyspacesStreamsShardSyncer.class.getDeclaredField("streamIdentifier");

            isMultiStreamModeField.setAccessible(true);
            streamIdentifierField.setAccessible(true);

            boolean isMultiStreamMode = (boolean) isMultiStreamModeField.get(multiStreamSyncer);
            String configuredStreamIdentifier = (String) streamIdentifierField.get(multiStreamSyncer);

            assertTrue(isMultiStreamMode);
            assertEquals(streamArn, configuredStreamIdentifier);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            fail("Failed to verify multi-stream initialization: " + e.getMessage());
        }
    }

    @Test
    void testMultiStreamSyncerWithMultipleStreams() throws Exception {
        // Setup multiple streams
        String stream1Arn = "arn:aws:cassandra:region:account:/keyspace/keyspace1/table/table1/stream/timestamp1";
        String stream2Arn = "arn:aws:cassandra:region:account:/keyspace/keyspace2/table/table2/stream/timestamp2";

        KeyspacesStreamsShardSyncer multiStreamSyncer1 = new KeyspacesStreamsShardSyncer(
                true,
                stream1Arn
        );

        KeyspacesStreamsShardSyncer multiStreamSyncer2 = new KeyspacesStreamsShardSyncer(
                true,
                stream2Arn
        );

        // Create timestamps and shard IDs
        long baseTimestamp = System.currentTimeMillis() - Duration.ofHours(7).toMillis();
        String shard1Id = String.format("shardId-%019d-001", baseTimestamp);
        String shard2Id = String.format("shardId-%019d-002", baseTimestamp);

        // Create test shards for each stream
        Shard shard1 = createTestShard(shard1Id, null, null, "0", null);
        Shard shard2 = createTestShard(shard2Id, null, null, "0", null);

        // Setup mock detectors for each stream
        KeyspacesStreamsShardDetector mockDetector1 = mock(KeyspacesStreamsShardDetector.class);
        KeyspacesStreamsShardDetector mockDetector2 = mock(KeyspacesStreamsShardDetector.class);

        when(mockDetector1.listShards()).thenReturn(Collections.singletonList(shard1));
        when(mockDetector2.listShards()).thenReturn(Collections.singletonList(shard2));

        when(mockDetector1.streamIdentifier()).thenReturn(StreamIdentifier.singleStreamInstance(stream1Arn));
        when(mockDetector2.streamIdentifier()).thenReturn(StreamIdentifier.singleStreamInstance(stream2Arn));

        // Mock LeaseRefresher
        LeaseRefresher mockLeaseRefresher = mock(LeaseRefresher.class);
        when(mockLeaseRefresher.listLeasesForStream(any(StreamIdentifier.class)))
                .thenReturn(Collections.emptyList());

        // Execute for both streams
        boolean result1 = multiStreamSyncer1.checkAndCreateLeaseForNewShards(
                mockDetector1,
                mockLeaseRefresher,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON),
                metricsScope,
                false,
                true
        );

        boolean result2 = multiStreamSyncer2.checkAndCreateLeaseForNewShards(
                mockDetector2,
                mockLeaseRefresher,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON),
                metricsScope,
                false,
                true
        );

        // Verify
        assertTrue(result1);
        assertTrue(result2);

        // Verify lease creation
        ArgumentCaptor<Lease> leaseCaptor = ArgumentCaptor.forClass(Lease.class);
        verify(mockLeaseRefresher, times(2)).createLeaseIfNotExists(leaseCaptor.capture());

        List<Lease> capturedLeases = leaseCaptor.getAllValues();
        assertEquals(2, capturedLeases.size());

        // Verify leases for different streams
        Set<String> capturedStreamIdentifiers = capturedLeases.stream()
                .map(lease -> ((MultiStreamLease) lease).streamIdentifier())
                .collect(Collectors.toSet());

        assertTrue(capturedStreamIdentifiers.contains(stream1Arn));
        assertTrue(capturedStreamIdentifiers.contains(stream2Arn));

        // Additional verification for shard IDs
        List<String> capturedShardIds = capturedLeases.stream()
                .map(lease -> ((MultiStreamLease) lease).shardId())
                .collect(Collectors.toList());

        // Assert the size is 2
        assertEquals(2, capturedShardIds.size());
        // Assert first element is shard1Id
        assertEquals(shard1Id, capturedShardIds.get(0));
        // Assert second element is shard2Id
        assertEquals(shard2Id, capturedShardIds.get(1));
    }

    @Test
    void testLeaseCreationForComplexLineageSingleLease() throws Exception {
        // Create base timestamp and shard IDs
        long baseTimestamp = System.currentTimeMillis() - Duration.ofHours(7).toMillis();
        String rootShardId = String.format("shardId-%019d-001", baseTimestamp);

        // Child shards created 1 second later
        String child1ShardId = String.format("shardId-%019d-002", baseTimestamp + 1000);
        String child2ShardId = String.format("shardId-%019d-003", baseTimestamp + 1000);

        // Grandchild shards created 2 seconds later
        String grandchild1ShardId = String.format("shardId-%019d-004", baseTimestamp + 2000);
        String grandchild2ShardId = String.format("shardId-%019d-005", baseTimestamp + 2000);
        String grandchild3ShardId = String.format("shardId-%019d-006", baseTimestamp + 2000);
        String grandchild4ShardId = String.format("shardId-%019d-007", baseTimestamp + 2000);
        String grandchild5ShardId = String.format("shardId-%019d-008", baseTimestamp + 2000);

        // Setup a complex shard lineage with proper sequence numbers
        Shard rootShard = createTestShard(rootShardId, null, null, "0", "100");  // closed shard
        Shard child1 = createTestShard(child1ShardId, rootShardId, null, "101", "200");  // closed shard
        Shard child2 = createTestShard(child2ShardId, rootShardId, null, "101", "200");  // closed shard
        Shard grandchild1 = createTestShard(grandchild1ShardId, child1ShardId, null, "201", null);  // open shard
        Shard grandchild2 = createTestShard(grandchild2ShardId, child1ShardId, null, "201", null);  // open shard
        Shard grandchild3 = createTestShard(grandchild3ShardId, child2ShardId, null, "201", null);  // open shard
        Shard grandchild4 = createTestShard(grandchild4ShardId, child2ShardId, null, "201", null);  // open shard
        Shard grandchild5 = createTestShard(grandchild5ShardId, child2ShardId, null, "201", null);  // open shard

        List<Shard> shards = Arrays.asList(rootShard, child1, child2, grandchild1,
                grandchild2, grandchild3, grandchild4, grandchild5);

        when(shardDetector.listShards()).thenReturn(shards);
        when(leaseRefresher.listLeases()).thenReturn(Collections.emptyList());

        // Execute
        boolean result = shardSyncer.checkAndCreateLeaseForNewShards(
                shardDetector,
                leaseRefresher,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON),
                metricsScope,
                false,
                true
        );

        // Verify
        assertTrue(result);

        ArgumentCaptor<Lease> leaseCaptor = ArgumentCaptor.forClass(Lease.class);
        verify(leaseRefresher, times(8)).createLeaseIfNotExists(leaseCaptor.capture());

        List<Lease> capturedLeases = leaseCaptor.getAllValues();
        assertEquals(8, capturedLeases.size());

        // Verify each lease
        Map<String, Lease> leaseMap = capturedLeases.stream()
                .collect(Collectors.toMap(Lease::leaseKey, l -> l));

        // Root shard should have TRIM_HORIZON checkpoint
        verifyLease(leaseMap.get(rootShardId), rootShardId,
                Collections.emptyList(), ExtendedSequenceNumber.TRIM_HORIZON);

        // Children of root should have TRIM_HORIZON checkpoint
        verifyLease(leaseMap.get(child1ShardId), child1ShardId,
                Collections.singletonList(rootShardId), ExtendedSequenceNumber.TRIM_HORIZON);
        verifyLease(leaseMap.get(child2ShardId), child2ShardId,
                Collections.singletonList(rootShardId), ExtendedSequenceNumber.TRIM_HORIZON);

        // Grandchildren should have TRIM_HORIZON checkpoint as they are descendants
        verifyLease(leaseMap.get(grandchild1ShardId), grandchild1ShardId,
                Collections.singletonList(child1ShardId), ExtendedSequenceNumber.TRIM_HORIZON);
        verifyLease(leaseMap.get(grandchild2ShardId), grandchild2ShardId,
                Collections.singletonList(child1ShardId), ExtendedSequenceNumber.TRIM_HORIZON);
        verifyLease(leaseMap.get(grandchild3ShardId), grandchild3ShardId,
                Collections.singletonList(child2ShardId), ExtendedSequenceNumber.TRIM_HORIZON);
        verifyLease(leaseMap.get(grandchild4ShardId), grandchild4ShardId,
                Collections.singletonList(child2ShardId), ExtendedSequenceNumber.TRIM_HORIZON);
        verifyLease(leaseMap.get(grandchild5ShardId), grandchild5ShardId,
                Collections.singletonList(child2ShardId), ExtendedSequenceNumber.TRIM_HORIZON);
    }

    @Test
    void testLeaseCreationForComplexLineageMultiLease() throws Exception {
        // Setup
        String streamArn = "arn:aws:cassandra:us-west-2:123456789012:/keyspace/TestKeyspace/table/TestTable/stream/2024-02-03T00:00:00.000";
        StreamIdentifier streamIdentifier = StreamIdentifier.singleStreamInstance(streamArn);

        KeyspacesStreamsShardSyncer multiStreamSyncer = new KeyspacesStreamsShardSyncer(
                true,
                streamArn
        );

        // Create base timestamp and shard IDs
        long baseTimestamp = System.currentTimeMillis() - Duration.ofHours(7).toMillis();
        String rootShardId = String.format("shardId-%019d-001", baseTimestamp);

        // Child shards created 1 second later
        String child1ShardId = String.format("shardId-%019d-002", baseTimestamp + 1000);
        String child2ShardId = String.format("shardId-%019d-003", baseTimestamp + 1000);

        // Grandchild shards created 2 seconds later
        String grandchild1ShardId = String.format("shardId-%019d-004", baseTimestamp + 2000);
        String grandchild2ShardId = String.format("shardId-%019d-005", baseTimestamp + 2000);
        String grandchild3ShardId = String.format("shardId-%019d-006", baseTimestamp + 2000);
        String grandchild4ShardId = String.format("shardId-%019d-007", baseTimestamp + 2000);
        String grandchild5ShardId = String.format("shardId-%019d-008", baseTimestamp + 2000);

        // Create shards with proper sequence numbers
        Shard rootShard = createTestShard(rootShardId, null, null, "0", "100");
        Shard child1 = createTestShard(child1ShardId, rootShardId, null, "101", "200");
        Shard child2 = createTestShard(child2ShardId, rootShardId, null, "101", "200");
        Shard grandchild1 = createTestShard(grandchild1ShardId, child1ShardId, null, "201", null);
        Shard grandchild2 = createTestShard(grandchild2ShardId, child1ShardId, null, "201", null);
        Shard grandchild3 = createTestShard(grandchild3ShardId, child2ShardId, null, "201", null);
        Shard grandchild4 = createTestShard(grandchild4ShardId, child2ShardId, null, "201", null);
        Shard grandchild5 = createTestShard(grandchild5ShardId, child2ShardId, null, "201", null);

        List<Shard> shards = Arrays.asList(rootShard, child1, child2, grandchild1,
                grandchild2, grandchild3, grandchild4, grandchild5);

        when(shardDetector.listShards()).thenReturn(shards);
        when(shardDetector.streamIdentifier()).thenReturn(streamIdentifier);
        when(leaseRefresher.listLeasesForStream(streamIdentifier)).thenReturn(Collections.emptyList());

        // Execute
        boolean result = multiStreamSyncer.checkAndCreateLeaseForNewShards(
                shardDetector,
                leaseRefresher,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON),
                metricsScope,
                false,
                true
        );

        // Verify
        assertTrue(result);

        ArgumentCaptor<Lease> leaseCaptor = ArgumentCaptor.forClass(Lease.class);
        verify(leaseRefresher, times(8)).createLeaseIfNotExists(leaseCaptor.capture());

        List<Lease> capturedLeases = leaseCaptor.getAllValues();
        assertEquals(8, capturedLeases.size());

        // Verify each lease
        Map<String, MultiStreamLease> leaseMap = capturedLeases.stream()
                .map(lease -> (MultiStreamLease) lease)
                .collect(Collectors.toMap(MultiStreamLease::shardId, l -> l));

        verifyMultiStreamLease(leaseMap.get(rootShardId), rootShardId, Collections.emptySet(),
                ExtendedSequenceNumber.TRIM_HORIZON, streamArn);
        verifyMultiStreamLease(leaseMap.get(child1ShardId), child1ShardId, Collections.singleton(rootShardId),
                ExtendedSequenceNumber.TRIM_HORIZON, streamArn);
        verifyMultiStreamLease(leaseMap.get(child2ShardId), child2ShardId, Collections.singleton(rootShardId),
                ExtendedSequenceNumber.TRIM_HORIZON, streamArn);
        verifyMultiStreamLease(leaseMap.get(grandchild1ShardId), grandchild1ShardId, Collections.singleton(child1ShardId),
                ExtendedSequenceNumber.TRIM_HORIZON, streamArn);
        verifyMultiStreamLease(leaseMap.get(grandchild2ShardId), grandchild2ShardId, Collections.singleton(child1ShardId),
                ExtendedSequenceNumber.TRIM_HORIZON, streamArn);
        verifyMultiStreamLease(leaseMap.get(grandchild3ShardId), grandchild3ShardId, Collections.singleton(child2ShardId),
                ExtendedSequenceNumber.TRIM_HORIZON, streamArn);
        verifyMultiStreamLease(leaseMap.get(grandchild4ShardId), grandchild4ShardId, Collections.singleton(child2ShardId),
                ExtendedSequenceNumber.TRIM_HORIZON, streamArn);
        verifyMultiStreamLease(leaseMap.get(grandchild5ShardId), grandchild5ShardId, Collections.singleton(child2ShardId),
                ExtendedSequenceNumber.TRIM_HORIZON, streamArn);
    }


    private void verifyLease(Lease lease, String expectedShardId, List<String> expectedParentShardIds, ExtendedSequenceNumber expectedCheckpoint) {
        assertNotNull(lease);
        assertEquals(expectedShardId, lease.leaseKey());
        // Convert both collections to sets for comparison
        assertEquals(new HashSet<>(expectedParentShardIds), new HashSet<>(lease.parentShardIds()));
        assertEquals(expectedCheckpoint, lease.checkpoint());
        assertEquals(0L, lease.ownerSwitchesSinceCheckpoint());
    }

    private void verifyMultiStreamLease(MultiStreamLease lease,
                                        String expectedShardId,
                                        Set<String> expectedParentShardIds,
                                        ExtendedSequenceNumber expectedCheckpoint,
                                        String expectedStreamArn) {
        assertNotNull(lease);
        assertEquals(expectedShardId, lease.shardId());
        assertEquals(expectedParentShardIds, new HashSet<>(lease.parentShardIds()));
        assertEquals(expectedCheckpoint, lease.checkpoint());
        assertEquals(0L, lease.ownerSwitchesSinceCheckpoint());
        assertEquals(expectedStreamArn, lease.streamIdentifier());
        assertEquals(MultiStreamLease.getLeaseKey(expectedStreamArn, expectedShardId), lease.leaseKey());
    }

    @Test
    void testLeaseCreationWithExistingLeaseAndAncestorRecursion() throws Exception {
        // Create timestamps for shard IDs
        long baseTimestamp = System.currentTimeMillis() - Duration.ofHours(7).toMillis();
        String grandparentShardId = String.format("shardId-%019d-001", baseTimestamp);
        String parentShardId = String.format("shardId-%019d-002", baseTimestamp + 1000);
        String child1ShardId = String.format("shardId-%019d-003", baseTimestamp + 2000);
        String child2ShardId = String.format("shardId-%019d-004", baseTimestamp + 2000);
        String independentShardId = String.format("shardId-%019d-005", baseTimestamp + 3000);

        // grandparent (closed, existing lease) -> parent (closed) -> child1 (open)
        //                                                        -> child2 (open, existing lease)
        // independent (open)

        Shard grandparent = createTestShard(grandparentShardId, null, null, "0", "100");  // closed
        Shard parent = createTestShard(parentShardId, grandparentShardId, null, "101", "200");  // closed
        Shard child1 = createTestShard(child1ShardId, parentShardId, null, "201", null);  // open
        Shard child2 = createTestShard(child2ShardId, parentShardId, null, "201", null);  // open, will have existing lease
        Shard independent = createTestShard(independentShardId, null, null, "0", null);  // open, no parent

        List<Shard> shards = Arrays.asList(grandparent, parent, child1, child2, independent);

        // Mock existing leases for grandparent and child2 - UPDATED with checkpoint and parentShardIds
        Lease existingGrandparentLease = new Lease();
        existingGrandparentLease.leaseKey(grandparentShardId);
        existingGrandparentLease.checkpoint(ExtendedSequenceNumber.SHARD_END);  // Add checkpoint
        existingGrandparentLease.parentShardIds(Collections.emptyList());  // Add parent info

        Lease existingChild2Lease = new Lease();
        existingChild2Lease.leaseKey(child2ShardId);
        existingChild2Lease.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);  // Add checkpoint
        existingChild2Lease.parentShardIds(Collections.singletonList(parentShardId));  // Add parent info

        when(leaseRefresher.listLeases()).thenReturn(Arrays.asList(existingGrandparentLease, existingChild2Lease));

        // Setup shard detector
        when(shardDetector.listShards()).thenReturn(shards);

        // Execute with TRIM_HORIZON position
        boolean result = shardSyncer.checkAndCreateLeaseForNewShards(
                shardDetector,
                leaseRefresher,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON),
                metricsScope,
                false,
                true
        );

        assertTrue(result);

        // Capture created leases
        ArgumentCaptor<Lease> leaseCaptor = ArgumentCaptor.forClass(Lease.class);
        verify(leaseRefresher, times(3)).createLeaseIfNotExists(leaseCaptor.capture());

        List<Lease> capturedLeases = leaseCaptor.getAllValues();
        Map<String, Lease> leaseMap = capturedLeases.stream()
                .collect(Collectors.toMap(Lease::leaseKey, l -> l));

        // Verify leases were created for parent, child1, and independent
        verifyLease(leaseMap.get(parentShardId), parentShardId,
                Collections.singletonList(grandparentShardId), ExtendedSequenceNumber.TRIM_HORIZON);
        verifyLease(leaseMap.get(child1ShardId), child1ShardId,
                Collections.singletonList(parentShardId), ExtendedSequenceNumber.TRIM_HORIZON);
        verifyLease(leaseMap.get(independentShardId), independentShardId,
                Collections.emptyList(), ExtendedSequenceNumber.TRIM_HORIZON);

        // Verify grandparent and child2 leases were not created (since they already existed)
        assertFalse(leaseMap.containsKey(grandparentShardId));
        assertFalse(leaseMap.containsKey(child2ShardId));

        // Verify the order of lease creation (parent should be created before child1)
        List<String> creationOrder = capturedLeases.stream()
                .map(Lease::leaseKey)
                .collect(Collectors.toList());
        assertTrue(creationOrder.indexOf(parentShardId) < creationOrder.indexOf(child1ShardId));
    }

    @Test
    void testCheckAndCreateLeasesForNewShardsWithInconsistentShardsFailure() {
        // Setup
        String parentShardId = "shardId-000";
        String childShardId = "shardId-001";

        // Create parent shard that's still open
        Shard parentShard = createTestShard(parentShardId, null, null);
        Shard childShard = createTestShard(childShardId, parentShardId, null);

        List<Shard> shards = Arrays.asList(parentShard, childShard);

        // Mock ShardDetector
        when(shardDetector.listShards()).thenReturn(shards);

        // Test with ignoreUnexpectedChildShards = false
        assertThrows(KinesisClientLibIOException.class, () ->
                shardSyncer.checkAndCreateLeaseForNewShards(
                        shardDetector,
                        leaseRefresher,
                        InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON),
                        metricsScope,
                        false, // ignoreUnexpectedChildShards
                        true
                )
        );
    }

    @Test
    void testCheckAndCreateLeasesForNewShardsWithInconsistentShard() throws Exception {
        // Setup
        String parentShardId = "shardId-000";
        String childShardId = "shardId-001";

        // Create parent shard that's still open (no ending sequence number)
        Shard parentShard = createTestShard(parentShardId, null, null);
        Shard childShard = createTestShard(childShardId, parentShardId, null);

        List<Shard> shards = Arrays.asList(parentShard, childShard);

        // Mock ShardDetector
        when(shardDetector.listShards()).thenReturn(shards);

        // Test with ignoreUnexpectedChildShards = true
        boolean result = shardSyncer.checkAndCreateLeaseForNewShards(
                shardDetector,
                leaseRefresher,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON),
                metricsScope,
                true, // ignoreUnexpectedChildShards
                true
        );

        assertTrue(result);


        //  In this test case only Open Parent Shard Lease will be created and not the Open child Shard Lease is created
        //  because Child-Shard is marked as Inconsistent due to open parent & we don't create Leases for Inconsistent
        // shards

        // Verify only parent shard lease is created
        ArgumentCaptor<Lease> leaseCaptor = ArgumentCaptor.forClass(Lease.class);
        verify(leaseRefresher, times(1)).createLeaseIfNotExists(leaseCaptor.capture());
        List<Lease> capturedLeases = leaseCaptor.getAllValues();
        assertTrue(capturedLeases.stream()
                .anyMatch(lease -> lease.leaseKey().equals(parentShardId)));
        assertFalse(capturedLeases.stream()
                .anyMatch(lease -> lease.leaseKey().equals(childShardId)));
    }

    @Test
    void testGarbageLeaseCleanupMultiStreamMode() throws Exception {
        // Setup stream identifiers
        String currentStreamArn = "arn:aws:cassandra:region:account:/keyspace/keyspace1/table/table1/stream/timestamp";
        String otherStreamArn = "arn:aws:cassandra:region:account:/keyspace/keyspace2/table/table2/stream/timestamp";
        StreamIdentifier currentStreamId = StreamIdentifier.singleStreamInstance(currentStreamArn);

        // Create timestamps for shard IDs
        long baseTimestamp = System.currentTimeMillis() - Duration.ofHours(7).toMillis();
        String grandparentShardId = String.format("shardId-%019d-001", baseTimestamp);
        String parentShardId = String.format("shardId-%019d-002", baseTimestamp + 1000);
        String child1ShardId = String.format("shardId-%019d-003", baseTimestamp + 2000);
        String child2ShardId = String.format("shardId-%019d-004", baseTimestamp + 2000);
        String independentShardId = String.format("shardId-%019d-005", baseTimestamp + 3000);
        String staleShardId = String.format("shardId-%019d-006", baseTimestamp - Duration.ofDays(1).toMillis()); // Old shard

        // Setup current shards in the stream
        Shard grandparent = createTestShard(grandparentShardId, null, null, "0", "100");  // closed
        Shard parent = createTestShard(parentShardId, grandparentShardId, null, "101", "200");  // closed
        Shard child1 = createTestShard(child1ShardId, parentShardId, null, "201", null);  // open
        Shard child2 = createTestShard(child2ShardId, parentShardId, null, "201", null);  // open
        Shard independent = createTestShard(independentShardId, null, null, "0", null);  // open, no parent

        List<Shard> currentShards = Arrays.asList(grandparent, parent, child1, child2, independent);

        // Setup existing leases in the lease table
        // 1. Active shard lease from current stream
        MultiStreamLease currentActiveLease = createTestLease(
                currentStreamArn,
                child1ShardId,
                ExtendedSequenceNumber.TRIM_HORIZON,
                parentShardId
        );

        // 2. Stale shard lease from current stream (should be deleted)
        MultiStreamLease currentStaleLease = createTestLease(
                currentStreamArn,
                staleShardId,
                ExtendedSequenceNumber.SHARD_END,
                null
        );

        // 3. Lease from different stream (should not be deleted)
        MultiStreamLease otherStreamLease = createTestLease(
                otherStreamArn,
                "shardId-000000000000000000007-other",
                ExtendedSequenceNumber.TRIM_HORIZON,
                null
        );

        // Setup mocks
        when(shardDetector.streamIdentifier()).thenReturn(currentStreamId);
        when(shardDetector.listShards()).thenReturn(currentShards);
        when(leaseRefresher.listLeasesForStream(currentStreamId))
                .thenReturn(Arrays.asList(currentActiveLease, currentStaleLease));

        KeyspacesStreamsShardSyncer multiStreamSyncer = new KeyspacesStreamsShardSyncer(
                true,
                currentStreamArn
        );

        // Execute
        boolean result = multiStreamSyncer.checkAndCreateLeaseForNewShards(
                shardDetector,
                leaseRefresher,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON),
                metricsScope,
                false,
                true
        );

        assertTrue(result);

        // Verify
        verify(leaseRefresher).deleteLease(currentStaleLease);
        verify(leaseRefresher, never()).deleteLease(currentActiveLease);
        verify(leaseRefresher, never()).deleteLease(otherStreamLease);
    }

    @Test
    void testMultiStreamLeaseCleanupForClosedShard() throws Exception {
        // Setup stream identifier
        String stream1Arn = "arn:aws:cassandra:region:account:/keyspace/keyspace1/table/table1/stream/timestamp1";
        StreamIdentifier stream1Id = StreamIdentifier.singleStreamInstance(stream1Arn);

        // Create timestamps for each generation of shards
        long baseTimestamp = System.currentTimeMillis() - Duration.ofHours(7).toMillis();
        String parentShardId = String.format("shardId-%019d-001", baseTimestamp);
        String child1ShardId = String.format("shardId-%019d-002", baseTimestamp + 1000);
        String child2ShardId = String.format("shardId-%019d-003", baseTimestamp + 1000);
        String grandChild1ShardId = String.format("shardId-%019d-004", baseTimestamp + 2000);
        String grandChild2ShardId = String.format("shardId-%019d-005", baseTimestamp + 2000);

        // Setup shards - make sure closed shards have open children
        Shard parent = createTestShard(parentShardId, null, null, "0", "100");  // closed
        Shard child1 = createTestShard(child1ShardId, parentShardId, null, "101", "200");  // closed
        Shard child2 = createTestShard(child2ShardId, parentShardId, null, "101", "200");  // closed
        Shard grandChild1 = createTestShard(grandChild1ShardId, child1ShardId, null, "201", null);  // open
        Shard grandChild2 = createTestShard(grandChild2ShardId, child2ShardId, null, "201", null);  // open
        List<Shard> currentShards = Arrays.asList(parent, child1, child2, grandChild1, grandChild2);

        // Setup leases for stream1 (current stream)
        // Parent lease - completed
        Lease parentLease = createCompletedLease(
                stream1Arn,
                parentShardId,
                null  // no parent
        );

        // Child leases - both completed
        Lease child1Lease = createCompletedLease(
                stream1Arn,
                child1ShardId,
                parentShardId
        );

        Lease child2Lease = createCompletedLease(
                stream1Arn,
                child2ShardId,
                parentShardId
        );

        // Grandchild leases - both active at TRIM_HORIZON
        Lease grandChild1Lease = createTestLease(
                stream1Arn,
                grandChild1ShardId,
                ExtendedSequenceNumber.TRIM_HORIZON,  // active lease
                child1ShardId
        );

        Lease grandChild2Lease = createTestLease(
                stream1Arn,
                grandChild2ShardId,
                ExtendedSequenceNumber.TRIM_HORIZON,  // active lease
                child2ShardId
        );

        // Setup current stream leases
        List<Lease> stream1Leases = Arrays.asList(parentLease, child1Lease, child2Lease,
                grandChild1Lease, grandChild2Lease);

        // Setup mocks
        when(shardDetector.streamIdentifier()).thenReturn(stream1Id);
        when(shardDetector.listShards()).thenReturn(currentShards);
        when(leaseRefresher.listLeasesForStream(stream1Id)).thenReturn(stream1Leases);

        KeyspacesStreamsShardSyncer multiStreamSyncer = new KeyspacesStreamsShardSyncer(
                true,
                stream1Arn
        );

        // Execute
        boolean result = multiStreamSyncer.checkAndCreateLeaseForNewShards(
                shardDetector,
                leaseRefresher,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON),
                metricsScope,
                false,
                true
        );

        assertTrue(result);

        // Verify
        // Should delete parent lease since all its children are at SHARD_END and have active children
        verify(leaseRefresher).deleteLease(parentLease);

        // Should NOT delete child leases since they have active children
        verify(leaseRefresher, never()).deleteLease(child1Lease);
        verify(leaseRefresher, never()).deleteLease(child2Lease);

        // Should NOT delete grandchild leases since they're active
        verify(leaseRefresher, never()).deleteLease(grandChild1Lease);
        verify(leaseRefresher, never()).deleteLease(grandChild2Lease);

        // Verify we only used listLeasesForStream for the current stream
        verify(leaseRefresher).listLeasesForStream(stream1Id);
        verify(leaseRefresher, never()).listLeases();
    }

    @Test
    void testLeaseRetentionPeriodCheckAndDontDelete() throws Exception {
        // Setup stream identifier
        String streamArn = "arn:aws:cassandra:region:account:/keyspace/keyspace1/table/table1/stream/timestamp1";
        StreamIdentifier streamId = StreamIdentifier.singleStreamInstance(streamArn);

        // Create timestamps for each level
        Instant grandparentTime = Instant.now().minus(Duration.ofMinutes(30)); // 30 minutes old
        Instant parentTime = grandparentTime.plusSeconds(1);    // 1 second after grandparent
        Instant childTime = parentTime.plusSeconds(1);          // 1 second after parent

        // Create shard IDs
        final String grandparentShardId = shardId(grandparentTime);
        final String parentShardId1 = shardId(parentTime);
        final String parentShardId2 = shardId(parentTime.plusMillis(1));
        final String childShardId1 = shardId(childTime);
        final String childShardId2 = shardId(childTime.plusMillis(1));
        final String childShardId3 = shardId(childTime.plusMillis(2));
        final String childShardId4 = shardId(childTime.plusMillis(3));

        // Setup shards - note that only leaf nodes (level 3) are open (null endingSequenceNumber)
        Shard grandparentShard = createTestShard(grandparentShardId, null, null, "0", "100");  // closed

        Shard parentShard1 = createTestShard(parentShardId1, grandparentShardId, null, "101", "200");  // closed
        Shard parentShard2 = createTestShard(parentShardId2, grandparentShardId, null, "201", "300");  // closed

        // Leaf nodes are open (no ending sequence number)
        Shard childShard1 = createTestShard(childShardId1, parentShardId1, null, "301", null);  // open
        Shard childShard2 = createTestShard(childShardId2, parentShardId1, null, "401", null);  // open
        Shard childShard3 = createTestShard(childShardId3, parentShardId2, null, "501", null);  // open
        Shard childShard4 = createTestShard(childShardId4, parentShardId2, null, "601", null);  // open

        List<Shard> currentShards = Arrays.asList(
                grandparentShard,
                parentShard1, parentShard2,
                childShard1, childShard2, childShard3, childShard4
        );

        // Setup leases
        // Grandparent lease - completed
        MultiStreamLease grandparentLease = createCompletedLease(
                streamArn,
                grandparentShardId,
                null  // no parent
        );

        // Parent leases - all completed
        MultiStreamLease parentLease1 = createCompletedLease(
                streamArn,
                parentShardId1,
                grandparentShardId
        );

        MultiStreamLease parentLease2 = createCompletedLease(
                streamArn,
                parentShardId2,
                grandparentShardId
        );

        // Child leases - all still active
        MultiStreamLease childLease1 = createActiveLease(
                streamArn,
                childShardId1,
                "301",  // Active processing
                parentShardId1
        );

        MultiStreamLease childLease2 = createActiveLease(
                streamArn,
                childShardId2,
                "401",  // Active processing
                parentShardId1
        );

        MultiStreamLease childLease3 = createActiveLease(
                streamArn,
                childShardId3,
                "501",  // Active processing
                parentShardId2
        );

        MultiStreamLease childLease4 = createActiveLease(
                streamArn,
                childShardId4,
                "601",  // Active processing
                parentShardId2
        );

        List<Lease> streamLeases = Arrays.asList(
                grandparentLease,
                parentLease1, parentLease2,
                childLease1, childLease2, childLease3, childLease4
        );

        // Setup mocks
        when(shardDetector.streamIdentifier()).thenReturn(streamId);
        when(shardDetector.listShards()).thenReturn(currentShards);
        when(leaseRefresher.listLeasesForStream(streamId)).thenReturn(streamLeases);

        KeyspacesStreamsShardSyncer multiStreamSyncer = new KeyspacesStreamsShardSyncer(
                true,
                streamArn
        );

        // Execute
        boolean result = multiStreamSyncer.checkAndCreateLeaseForNewShards(
                shardDetector,
                leaseRefresher,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON),
                metricsScope,
                false,
                true
        );

        assertTrue(result);

        // Verify lease deletion behavior
        // Grandparent lease should not be deleted because:
        // 1. It's too recent (30 minutes < 6 hours)
        // 2. Its children (parents) are still being processed
        verify(leaseRefresher, never()).deleteLease(grandparentLease);

        // Parent leases should not be deleted because:
        // 1. They're even more recent than grandparent
        // 2. Their children (leaf nodes) are still being processed
        verify(leaseRefresher, never()).deleteLease(parentLease1);
        verify(leaseRefresher, never()).deleteLease(parentLease2);

        // Child leases should not be deleted because:
        // 1. They're the most recent
        // 2. They're still actively processing (not at SHARD_END)
        // 3. They're open shards (no ending sequence number)
        verify(leaseRefresher, never()).deleteLease(childLease1);
        verify(leaseRefresher, never()).deleteLease(childLease2);
        verify(leaseRefresher, never()).deleteLease(childLease3);
        verify(leaseRefresher, never()).deleteLease(childLease4);
    }

    @Test
    void testLeaseRetentionPeriodCheckAndDeleteGrandparent() throws Exception {
        // Setup stream identifier
        String streamArn = "arn:aws:cassandra:region:account:/keyspace/keyspace1/table/table1/stream/timestamp1";
        StreamIdentifier streamId = StreamIdentifier.singleStreamInstance(streamArn);

        // Create timestamps - grandparent old enough to be deleted (> 6 hours)
        Instant grandparentTime = Instant.now().minus(Duration.ofHours(7)); // 7 hours old
        Instant parentTime = Instant.now().minus(Duration.ofHours(3));      // 3 hours old
        Instant childTime = Instant.now().minus(Duration.ofHours(1));       // 1 hour old

        // Create shard IDs
        final String grandparentShardId = shardId(grandparentTime);
        final String parentShardId1 = shardId(parentTime);
        final String parentShardId2 = shardId(parentTime.plusMillis(1));
        final String childShardId1 = shardId(childTime);
        final String childShardId2 = shardId(childTime.plusMillis(1));
        final String childShardId3 = shardId(childTime.plusMillis(2));
        final String childShardId4 = shardId(childTime.plusMillis(3));

        // Setup shards - leaf nodes are open
        Shard grandparentShard = createTestShard(grandparentShardId, null, null, "0", "100");  // closed

        Shard parentShard1 = createTestShard(parentShardId1, grandparentShardId, null, "101", "200");  // closed
        Shard parentShard2 = createTestShard(parentShardId2, grandparentShardId, null, "201", "300");  // closed

        Shard childShard1 = createTestShard(childShardId1, parentShardId1, null, "301", null);  // open
        Shard childShard2 = createTestShard(childShardId2, parentShardId1, null, "401", null);  // open
        Shard childShard3 = createTestShard(childShardId3, parentShardId2, null, "501", null);  // open
        Shard childShard4 = createTestShard(childShardId4, parentShardId2, null, "601", null);  // open

        List<Shard> currentShards = Arrays.asList(
                grandparentShard,
                parentShard1, parentShard2,
                childShard1, childShard2, childShard3, childShard4
        );

        // Setup leases
        // Grandparent lease - old and completed
        MultiStreamLease grandparentLease = createCompletedLease(
                streamArn,
                grandparentShardId,
                null  // no parent
        );

        // Parent leases - all completed but not old enough
        MultiStreamLease parentLease1 = createCompletedLease(
                streamArn,
                parentShardId1,
                grandparentShardId
        );

        MultiStreamLease parentLease2 = createCompletedLease(
                streamArn,
                parentShardId2,
                grandparentShardId
        );

        // Child leases - still active
        MultiStreamLease childLease1 = createActiveLease(
                streamArn,
                childShardId1,
                "301",  // Still processing
                parentShardId1
        );

        MultiStreamLease childLease2 = createActiveLease(
                streamArn,
                childShardId2,
                "401",  // Still processing
                parentShardId1
        );

        MultiStreamLease childLease3 = createActiveLease(
                streamArn,
                childShardId3,
                "501",  // Still processing
                parentShardId2
        );

        MultiStreamLease childLease4 = createActiveLease(
                streamArn,
                childShardId4,
                "601",  // Still processing
                parentShardId2
        );

        List<Lease> streamLeases = Arrays.asList(
                grandparentLease,
                parentLease1, parentLease2,
                childLease1, childLease2, childLease3, childLease4
        );

        // Setup mocks
        when(shardDetector.streamIdentifier()).thenReturn(streamId);
        when(shardDetector.listShards()).thenReturn(currentShards);
        when(leaseRefresher.listLeasesForStream(streamId)).thenReturn(streamLeases);

        KeyspacesStreamsShardSyncer multiStreamSyncer = new KeyspacesStreamsShardSyncer(
                true,
                streamArn
        );

        // Execute
        boolean result = multiStreamSyncer.checkAndCreateLeaseForNewShards(
                shardDetector,
                leaseRefresher,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON),
                metricsScope,
                false,
                true
        );

        assertTrue(result);

        // Verify only grandparent lease is deleted because:
        // 1. It's old enough (7 hours > 6 hours)
        // 2. All its children (parents) are at SHARD_END
        verify(leaseRefresher, times(1)).deleteLease(grandparentLease);

        // Parent leases should not be deleted because they're not old enough
        verify(leaseRefresher, never()).deleteLease(parentLease1);
        verify(leaseRefresher, never()).deleteLease(parentLease2);

        // Child leases should not be deleted because:
        // 1. They're recent
        // 2. They're still processing (not at SHARD_END)
        // 3. They're open shards
        verify(leaseRefresher, never()).deleteLease(childLease1);
        verify(leaseRefresher, never()).deleteLease(childLease2);
        verify(leaseRefresher, never()).deleteLease(childLease3);
        verify(leaseRefresher, never()).deleteLease(childLease4);
    }

    @Test
    void testLeaseRetentionPeriodGrandparentNotDeletedDueToActiveChild() throws Exception {
        // Setup stream identifier
        String streamArn = "arn:aws:cassandra:region:account:/keyspace/keyspace1/table/table1/stream/timestamp1";
        StreamIdentifier streamId = StreamIdentifier.singleStreamInstance(streamArn);

        // Create timestamps - grandparent old enough to be deleted (> 6 hours) but won't be due to active child
        Instant grandparentTime = Instant.now().minus(Duration.ofHours(7)); // 7 hours old
        Instant parentTime = Instant.now().minus(Duration.ofHours(3));      // 3 hours old
        Instant childTime = Instant.now().minus(Duration.ofHours(1));       // 1 hour old

        // Create shard IDs
        final String grandparentShardId = shardId(grandparentTime);
        final String parentShardId1 = shardId(parentTime);                  // This parent will be at SHARD_END
        final String parentShardId2 = shardId(parentTime.plusMillis(1));    // This parent will still be processing
        final String childShardId1 = shardId(childTime);
        final String childShardId2 = shardId(childTime.plusMillis(1));
        final String childShardId3 = shardId(childTime.plusMillis(2));
        final String childShardId4 = shardId(childTime.plusMillis(3));

        // Setup shards - leaf nodes are open
        Shard grandparentShard = createTestShard(grandparentShardId, null, null, "0", "100");  // closed

        Shard parentShard1 = createTestShard(parentShardId1, grandparentShardId, null, "101", "200");  // closed
        Shard parentShard2 = createTestShard(parentShardId2, grandparentShardId, null, "201", "300");  // closed

        Shard childShard1 = createTestShard(childShardId1, parentShardId1, null, "301", null);  // open
        Shard childShard2 = createTestShard(childShardId2, parentShardId1, null, "401", null);  // open
        Shard childShard3 = createTestShard(childShardId3, parentShardId2, null, "501", null);  // open
        Shard childShard4 = createTestShard(childShardId4, parentShardId2, null, "601", null);  // open

        List<Shard> currentShards = Arrays.asList(
                grandparentShard,
                parentShard1, parentShard2,
                childShard1, childShard2, childShard3, childShard4
        );

        // Setup leases
        // Grandparent lease - old enough but won't be deleted due to active child
        MultiStreamLease grandparentLease = createCompletedLease(
                streamArn,
                grandparentShardId,
                null  // no parent
        );

        // Parent leases - one completed, one still processing
        MultiStreamLease parentLease1 = createCompletedLease(
                streamArn,
                parentShardId1,
                grandparentShardId
        );

        MultiStreamLease parentLease2 = createActiveLease(
                streamArn,
                parentShardId2,
                "250",  // Still processing
                grandparentShardId
        );

        // Child leases - all still active
        MultiStreamLease childLease1 = createActiveLease(
                streamArn,
                childShardId1,
                "301",
                parentShardId1
        );

        MultiStreamLease childLease2 = createActiveLease(
                streamArn,
                childShardId2,
                "401",
                parentShardId1
        );

        MultiStreamLease childLease3 = createActiveLease(
                streamArn,
                childShardId3,
                "501",
                parentShardId2
        );

        MultiStreamLease childLease4 = createActiveLease(
                streamArn,
                childShardId4,
                "601",
                parentShardId2
        );


        List<Lease> streamLeases = Arrays.asList(
                grandparentLease,
                parentLease1, parentLease2,
                childLease1, childLease2, childLease3, childLease4
        );

        // Setup mocks
        when(shardDetector.streamIdentifier()).thenReturn(streamId);
        when(shardDetector.listShards()).thenReturn(currentShards);
        when(leaseRefresher.listLeasesForStream(streamId)).thenReturn(streamLeases);

        KeyspacesStreamsShardSyncer multiStreamSyncer = new KeyspacesStreamsShardSyncer(
                true,
                streamArn
        );

        // Execute
        boolean result = multiStreamSyncer.checkAndCreateLeaseForNewShards(
                shardDetector,
                leaseRefresher,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON),
                metricsScope,
                false,
                true
        );

        assertTrue(result);

        // Verify grandparent lease is NOT deleted because:
        // 1. Although it's old enough (7 hours > 6 hours)
        // 2. One of its children (parentLease2) is still processing
        verify(leaseRefresher, never()).deleteLease(grandparentLease);

        // Parent leases should not be deleted
        verify(leaseRefresher, never()).deleteLease(parentLease1);  // Not old enough
        verify(leaseRefresher, never()).deleteLease(parentLease2);  // Still processing

        // Child leases should not be deleted
        verify(leaseRefresher, never()).deleteLease(childLease1);
        verify(leaseRefresher, never()).deleteLease(childLease2);
        verify(leaseRefresher, never()).deleteLease(childLease3);
        verify(leaseRefresher, never()).deleteLease(childLease4);

        // Additional verification that no other leases were deleted
        verify(leaseRefresher, never()).deleteLease(any());
    }
}