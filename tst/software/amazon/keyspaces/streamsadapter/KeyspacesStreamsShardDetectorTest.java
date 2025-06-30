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

import static software.amazon.keyspaces.streamsadapter.util.TestUtils.createShard;
import static software.amazon.keyspaces.streamsadapter.util.TestUtils.findShardById;
import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import software.amazon.keyspaces.streamsadapter.util.KinesisMapperUtil;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import software.amazon.awssdk.services.keyspacesstreams.model.StreamStatus;
import software.amazon.awssdk.services.keyspacesstreams.model.GetStreamRequest;
import software.amazon.awssdk.services.keyspacesstreams.model.GetStreamResponse;
import software.amazon.awssdk.services.kinesis.model.SequenceNumberRange;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.kinesis.common.StreamIdentifier;

import java.lang.reflect.Field;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class KeyspacesStreamsShardDetectorTest {

    @Mock
    private AmazonKeyspacesStreamsAdapterClient keyspacesStreamsClient;

    private KeyspacesStreamsShardDetector shardDetector;
    private static final String STREAM_ARN = "arn:aws:cassandra:us-west-2:123456789012:/keyspace/test_keyspace/table/Music/stream/2023-06-15T00:00:00.000";
    private static final StreamIdentifier STREAM_IDENTIFIER = StreamIdentifier.singleStreamInstance(KinesisMapperUtil.createKinesisStreamIdentifierFromKeyspacesStreamsArn(STREAM_ARN, false));

    // Configuration constants matching constructor values
    private static final long CACHE_TTL_SECONDS = 300L;
    private static final int MAX_CACHE_MISSES = 1;
    private static final int CACHE_MISS_WARNING_MODULUS = 100;

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);
        shardDetector = new KeyspacesStreamsShardDetector(
                keyspacesStreamsClient,
                STREAM_IDENTIFIER,
                CACHE_TTL_SECONDS,
                MAX_CACHE_MISSES,
                CACHE_MISS_WARNING_MODULUS,
                Duration.ofSeconds(30)
        );
    }

    @Test
    void testListShardsSuccess() {
        // Setup
        software.amazon.awssdk.services.keyspacesstreams.model.Shard keyspacesShard1 =
                software.amazon.awssdk.services.keyspacesstreams.model.Shard.builder()
                        .shardId("shard-1")
                        .parentShardIds(Collections.singletonList(null))
                        .sequenceNumberRange(
                                software.amazon.awssdk.services.keyspacesstreams.model.SequenceNumberRange.builder()
                                        .startingSequenceNumber("1")
                                        .endingSequenceNumber("100")
                                        .build()
                        )
                        .build();

        GetStreamResponse response = GetStreamResponse.builder()
                .shards(Collections.singletonList(keyspacesShard1))
                .nextToken(null)
                .streamStatus("ENABLED")
                .build();

        when(keyspacesStreamsClient.getStream(any(GetStreamRequest.class)))
                .thenReturn(CompletableFuture.supplyAsync(() -> response));

        // Execute
        List<Shard> shards = shardDetector.listShards();

        // Verify
        assertNotNull(shards, "Shards list should not be null");
        assertEquals(1, shards.size(), "Should have exactly one shard");

        Shard resultShard = shards.get(0);
        assertEquals("shard-1", resultShard.shardId());
        assertNull(resultShard.parentShardId());
        assertEquals("1", resultShard.sequenceNumberRange().startingSequenceNumber());
        assertEquals("100", resultShard.sequenceNumberRange().endingSequenceNumber());

        verify(keyspacesStreamsClient, times(1)).getStream(any(GetStreamRequest.class));
    }

    @Test
    void testShardCaching() {
        // Setup with complete shard object
        software.amazon.awssdk.services.keyspacesstreams.model.Shard keyspacesShard =
                software.amazon.awssdk.services.keyspacesstreams.model.Shard.builder()
                        .shardId("shard-1")
                        .sequenceNumberRange(
                                software.amazon.awssdk.services.keyspacesstreams.model.SequenceNumberRange.builder()
                                        .startingSequenceNumber("1")
                                        .endingSequenceNumber("100")
                                        .build()
                        )
                        .build();

        GetStreamResponse response = GetStreamResponse.builder()
                .shards(Collections.singletonList(keyspacesShard))
                .nextToken(null)
                .streamStatus("ENABLED")
                .build();

        when(keyspacesStreamsClient.getStream(any(GetStreamRequest.class)))
                .thenReturn(CompletableFuture.supplyAsync(() -> response));

        // First call - should make API call
        Shard shard1 = shardDetector.shard("shard-1");
        assertNotNull(shard1, "First shard lookup should not be null");

        // Second call - should use cache
        Shard shard2 = shardDetector.shard("shard-1");
        assertNotNull(shard2, "Second shard lookup should not be null");

        verify(keyspacesStreamsClient, times(1)).getStream(any(GetStreamRequest.class));
    }

    @Test
    void testPaginatedListShards() {
        // Setup first page
        software.amazon.awssdk.services.keyspacesstreams.model.Shard shard1 =
                software.amazon.awssdk.services.keyspacesstreams.model.Shard.builder()
                        .shardId("shard-1")
                        .sequenceNumberRange(
                                software.amazon.awssdk.services.keyspacesstreams.model.SequenceNumberRange.builder()
                                        .startingSequenceNumber("1")
                                        .endingSequenceNumber("100")
                                        .build()
                        )
                        .build();

        GetStreamResponse response1 = GetStreamResponse.builder()
                .shards(Collections.singletonList(shard1))
                .nextToken("token1")
                .streamStatus("ENABLED")
                .build();

        // Setup second page
        software.amazon.awssdk.services.keyspacesstreams.model.Shard shard2 =
                software.amazon.awssdk.services.keyspacesstreams.model.Shard.builder()
                        .shardId("shard-2")
                        .parentShardIds(Collections.singletonList("shard-1"))
                        .sequenceNumberRange(
                                software.amazon.awssdk.services.keyspacesstreams.model.SequenceNumberRange.builder()
                                        .startingSequenceNumber("101")
                                        .build()
                        )
                        .build();

        GetStreamResponse response2 = GetStreamResponse.builder()
                .shards(Collections.singletonList(shard2))
                .nextToken(null)
                .streamStatus("ENABLED")
                .build();

        when(keyspacesStreamsClient.getStream(any(GetStreamRequest.class)))
                .thenReturn(CompletableFuture.supplyAsync(() -> response1))
                .thenReturn(CompletableFuture.supplyAsync(() -> response2));

        // Execute
        List<Shard> shards = shardDetector.listShards();

        // Verify
        assertNotNull(shards, "Shards list should not be null");
        assertEquals(2, shards.size(), "Should have two shards");
        assertEquals("shard-1", shards.get(0).shardId());
        assertEquals("shard-2", shards.get(1).shardId());
        verify(keyspacesStreamsClient, times(2)).getStream(any(GetStreamRequest.class));
    }

    @Test
    void testInitialCacheMiss() {
        // Setup - empty response
        GetStreamResponse emptyResponse = GetStreamResponse.builder()
                .shards(Collections.emptyList())
                .nextToken(null)
                .streamStatus("ENABLED")
                .build();

        when(keyspacesStreamsClient.getStream(any(GetStreamRequest.class)))
                .thenReturn(CompletableFuture.supplyAsync(() -> emptyResponse));

        // Test initial cache miss
        Shard result = shardDetector.shard("non-existent-shard");

        // Verify
        assertNull(result);
        assertEquals(1, shardDetector.cacheMisses().get(), "First cache miss should increment counter");
        verify(keyspacesStreamsClient, times(1))
                .getStream(any(GetStreamRequest.class));
    }

    @Test
    void testCacheMissExceedingMaxRetries() {
        // Setup
        GetStreamResponse emptyResponse = GetStreamResponse.builder()
                .shards(Collections.emptyList())
                .nextToken(null)
                .streamStatus("ENABLED")
                .build();

        when(keyspacesStreamsClient.getStream(any(GetStreamRequest.class)))
                .thenReturn(CompletableFuture.supplyAsync(() -> emptyResponse));

        // First call to initialize cache
        Shard result = shardDetector.shard("non-existent-shard");
        assertNull(result);
        assertEquals(1, shardDetector.cacheMisses().get());

        // Reset mock for next test
        reset(keyspacesStreamsClient);
        when(keyspacesStreamsClient.getStream(any(GetStreamRequest.class)))
                .thenReturn(CompletableFuture.supplyAsync(() -> emptyResponse));

        // Exceed maxCacheMissesBeforeReload (which is 1)
        result = shardDetector.shard("non-existent-shard");

        // Verify
        assertNull(result);
        assertEquals(0, shardDetector.cacheMisses().get(), "Cache misses should reset after refresh");
        verify(keyspacesStreamsClient, times(2))
                .getStream(any(GetStreamRequest.class));
    }

    @Test
    void testCacheMissWithAgeBasedRefresh() throws Exception {
        // Setup
        GetStreamResponse emptyResponse = GetStreamResponse.builder()
                .shards(Collections.emptyList())
                .nextToken(null)
                .streamStatus("ENABLED")
                .build();

        when(keyspacesStreamsClient.getStream(any(GetStreamRequest.class)))
                .thenReturn(CompletableFuture.supplyAsync(() -> emptyResponse));

        // First call to initialize cache
        shardDetector.shard("non-existent-shard");

        // Force cache to be old
        Field lastCacheUpdateTimeField = KeyspacesStreamsShardDetector.class
                .getDeclaredField("lastCacheUpdateTime");
        lastCacheUpdateTimeField.setAccessible(true);
        lastCacheUpdateTimeField.set(shardDetector,
                Instant.now().minus(Duration.ofSeconds(CACHE_TTL_SECONDS + 1)));

        // Reset mock for clean verification
        reset(keyspacesStreamsClient);
        when(keyspacesStreamsClient.getStream(any(GetStreamRequest.class)))
                .thenReturn(CompletableFuture.supplyAsync(() -> emptyResponse));

        // Test cache miss with old cache
        Shard result = shardDetector.shard("non-existent-shard");

        // Verify
        assertNull(result);
        assertEquals(0, shardDetector.cacheMisses().get(), "Cache misses should reset after age-based refresh");
        verify(keyspacesStreamsClient, times(2))
                .getStream(any(GetStreamRequest.class));
    }

    @Test
    public void testCacheRefreshDueToTimeExpiry() throws InterruptedException {
        // Setup detector with short TTL
        AmazonKeyspacesStreamsAdapterClient mockClient = mock(AmazonKeyspacesStreamsAdapterClient.class);
        StreamIdentifier streamId = StreamIdentifier.singleStreamInstance(
                KinesisMapperUtil.createKinesisStreamIdentifierFromKeyspacesStreamsArn(
                        "arn:aws:cassandra:us-west-2:123456789012:/keyspace/test_keyspace/table/Music/stream/2024-01-15T00:00:00.000",
                        false
                ));

        KeyspacesStreamsShardDetector detector = createDetectorWithShortTTL(mockClient, streamId);

        // Create shards
        Shard firstShard = createCompleteShard(
                "shard-001",
                null,
                "00000000000000000000",
                "99999999999999999999"
        );

        Shard secondShard = createCompleteShard(
                "shard-002",
                "shard-001",
                "10000000000000000000",
                null  // Active shard
        );

        // Create responses
        GetStreamResponse initialResponse = createStreamResponse(Collections.singletonList(firstShard));
        GetStreamResponse updatedResponse = createStreamResponse(Arrays.asList(firstShard, secondShard));

        // Setup mock behavior
        when(mockClient.getStream(any(GetStreamRequest.class)))
                .thenReturn(CompletableFuture.supplyAsync(() -> initialResponse))
                .thenReturn(CompletableFuture.supplyAsync(() -> updatedResponse));

        // First call - should populate cache
        Shard firstShardResult = detector.shard("shard-001");
        assertNotNull(firstShardResult);
        verify(mockClient, times(1)).getStream(any(GetStreamRequest.class));

        // Wait for cache to expire
        Thread.sleep(3000);

        // Verify we can now get the new shard
        Shard newShardResult = detector.shard("shard-002");
        assertNotNull(newShardResult);
        verify(mockClient, times(2)).getStream(any(GetStreamRequest.class));
    }

    @Test
    void testHandlingOpenParentShards() {
        // Create test shards
        Shard openParentShard =
                createShard("parent-1", null, "1", null);

        Shard childShard1 =
                createShard("child-1", "parent-1", "100", null);

        Shard childShard2 =
                createShard("child-2", "parent-1", "201", null);

        // Create response
        GetStreamResponse response = GetStreamResponse.builder()
                .shards(Arrays.asList(
                        convertShardToKeyspacesShard(openParentShard),
                        convertShardToKeyspacesShard(childShard1),
                        convertShardToKeyspacesShard(childShard2)
                ))
                .nextToken(null)
                .streamStatus("ENABLED")
                .build();

        when(keyspacesStreamsClient.getStream(any(GetStreamRequest.class)))
                .thenReturn(CompletableFuture.supplyAsync(() -> response));

        // Execute
        List<Shard> shards = shardDetector.listShards();

        // Verify basic conditions
        assertNotNull(shards, "Shards list should not be null");
        assertEquals(3, shards.size(), "Should have three shards");

        // Verify parent shard (should be closed due to having children)
        verifyShardClosure("parent-1", shards, true, "1");

        // Verify child shards (should remain open)
        verifyShardState("child-1", shards, "100", null);
        verifyShardState("child-2", shards, "201", null);

        verify(keyspacesStreamsClient, times(1))
                .getStream(any(GetStreamRequest.class));
    }

    @Test
    void testDisabledStreamLeafNodeHandling() {
        // Given - Create a stream with all closed shards
        Shard parentShard =
                createShard("parent-1", null, "100", "200");  // Closed parent

        Shard intermediateShard =
                createShard("child-1", "parent-1", "200", "300");  // Closed intermediate

        Shard closedLeafShard1 =
                createShard("leaf-1", "parent-1", "300", "400");  // Closed leaf with parent

        Shard closedLeafShard2 =
                createShard("leaf-2", "child-1", "500", "600");  // Closed leaf with intermediate parent

        Shard closedLeafShard3 =
                createShard("leaf-3", "child-1", "700", "800");  // Closed leaf with intermediate parent

        List<Shard> shards = Arrays.asList(
                parentShard, intermediateShard, closedLeafShard1, closedLeafShard2, closedLeafShard3
        );

        // Create response for disabled stream
        GetStreamResponse response = GetStreamResponse.builder()
                .shards(shards.stream().map(this::convertShardToKeyspacesShard).collect(Collectors.toList()))
                .nextToken(null)
                .streamStatus("DISABLED")  // Stream is disabled
                .build();

        when(keyspacesStreamsClient.getStream(any(GetStreamRequest.class)))
                .thenReturn(CompletableFuture.supplyAsync(() -> response));

        // When
        List<Shard> resultShards = shardDetector.listShards();

        // Then
        assertNotNull(resultShards);
        assertEquals(5, resultShards.size(), "Should maintain all shards");

        // Verify non-leaf nodes remain closed
        Shard resultParent = findShardById(resultShards, "parent-1");
        assertNotNull(resultParent);
        assertEquals("200", resultParent.sequenceNumberRange().endingSequenceNumber(),
                "Parent shard should remain closed");

        Shard resultIntermediate = findShardById(resultShards, "child-1");
        assertNotNull(resultIntermediate);
        assertEquals("300", resultIntermediate.sequenceNumberRange().endingSequenceNumber(),
                "Intermediate shard should remain closed");

        // Verify leaf nodes are opened
        verifyLeafIsOpen(resultShards, "leaf-1");
        verifyLeafIsOpen(resultShards, "leaf-2");
        verifyLeafIsOpen(resultShards, "leaf-3");

        // Verify parent relationships are maintained
        Shard leaf1 = findShardById(resultShards, "leaf-1");
        assertEquals("parent-1", leaf1.parentShardId(), "Leaf-1 should maintain parent relationship");

        Shard leaf2 = findShardById(resultShards, "leaf-2");
        assertEquals("child-1", leaf2.parentShardId(), "Leaf-2 should maintain intermediate parent relationship");

        Shard leaf3 = findShardById(resultShards, "leaf-3");
        assertEquals("child-1", leaf3.parentShardId(), "Leaf-3 should maintain intermediate parent relationship");

        // Verify get stream was called exactly once
        verify(keyspacesStreamsClient, times(1))
                .getStream(any(GetStreamRequest.class));
    }

    @Test
    void testNextTokenExtraction() {
        // Create test shards
        Shard shard1 = createShard("shard-1", null, "1", "100");
        Shard shard2 = createShard("shard-2", "shard-1", "101", null);

        // Create response with nextToken
        GetStreamResponse response1 = GetStreamResponse.builder()
                .shards(Collections.singletonList(convertShardToKeyspacesShard(shard1)))
                .nextToken("nextToken")
                .streamStatus("ENABLED")
                .build();

        GetStreamResponse response2 = GetStreamResponse.builder()
                .shards(Collections.singletonList(convertShardToKeyspacesShard(shard2)))
                .nextToken(null)
                .streamStatus("ENABLED")
                .build();

        when(keyspacesStreamsClient.getStream(any(GetStreamRequest.class)))
                .thenReturn(CompletableFuture.supplyAsync(() -> response1))
                .thenReturn(CompletableFuture.supplyAsync(() -> response2));

        // Execute
        List<Shard> shards = shardDetector.listShards();

        // Verify
        assertNotNull(shards);
        assertEquals(2, shards.size());
        verify(keyspacesStreamsClient, times(2)).getStream(any(GetStreamRequest.class));
    }

    // Helper methods
    private KeyspacesStreamsShardDetector createDetectorWithShortTTL(
            AmazonKeyspacesStreamsAdapterClient client,
            StreamIdentifier streamId) {
        return new KeyspacesStreamsShardDetector(
                client,
                streamId,
                2L,
                10,
                5,
                Duration.ofSeconds(30)
        );
    }

    private Shard createCompleteShard(
            String shardId,
            String parentShardId,
            String startSeq,
            String endSeq) {
        return Shard.builder()
                .shardId(shardId)
                .parentShardId(parentShardId)
                .sequenceNumberRange(
                        SequenceNumberRange.builder()
                                .startingSequenceNumber(startSeq)
                                .endingSequenceNumber(endSeq)
                                .build()
                )
                .build();
    }

    private GetStreamResponse createStreamResponse(
            List<Shard> shards) {
        return GetStreamResponse.builder()
                .shards(shards.stream().map(this::convertShardToKeyspacesShard).collect(Collectors.toList()))
                .nextToken(null)
                .streamStatus(StreamStatus.ENABLED.toString())
                .build();
    }

    private software.amazon.awssdk.services.keyspacesstreams.model.Shard convertShardToKeyspacesShard(Shard shard) {
        return software.amazon.awssdk.services.keyspacesstreams.model.Shard.builder()
                .shardId(shard.shardId())
                .parentShardIds(shard.parentShardId() != null ? Collections.singletonList(shard.parentShardId()) : Collections.emptyList())
                .sequenceNumberRange(
                        software.amazon.awssdk.services.keyspacesstreams.model.SequenceNumberRange.builder()
                                .startingSequenceNumber(shard.sequenceNumberRange().startingSequenceNumber())
                                .endingSequenceNumber(shard.sequenceNumberRange().endingSequenceNumber())
                                .build()
                )
                .build();
    }

    //  Helper methods for verification
    private void verifyShardClosure(String shardId, List<Shard> shards, boolean shouldBeClosed,
                                    String expectedStartSeq) {
        Shard shard = findShardById(shards, shardId);
        assertNotNull(shard, "Shard " + shardId + " should exist");
        assertEquals(expectedStartSeq, shard.sequenceNumberRange().startingSequenceNumber(),
                "Shard " + shardId + " should have correct starting sequence");
        if (shouldBeClosed) {
            assertEquals(String.valueOf(Long.MAX_VALUE),
                    shard.sequenceNumberRange().endingSequenceNumber(),
                    "Shard " + shardId + " should be closed with MAX_SEQUENCE_NUMBER");
        }
    }

    private void verifyShardState(String shardId, List<Shard> shards,
                                  String expectedStartSeq, String expectedEndSeq) {
        Shard shard = findShardById(shards, shardId);
        assertNotNull(shard, "Shard " + shardId + " should exist");
        assertEquals(expectedStartSeq, shard.sequenceNumberRange().startingSequenceNumber(),
                "Shard " + shardId + " should have correct starting sequence");
        assertEquals(expectedEndSeq, shard.sequenceNumberRange().endingSequenceNumber(),
                "Shard " + shardId + " should have correct ending sequence");
    }

    private void verifyLeafIsOpen(List<Shard> shards, String shardId) {
        Shard leaf = findShardById(shards, shardId);
        assertNotNull(leaf, "Leaf shard " + shardId + " should exist");
        assertEquals(null, leaf.sequenceNumberRange().endingSequenceNumber(),
                "Leaf shard " + shardId + " should be open");
    }
}
