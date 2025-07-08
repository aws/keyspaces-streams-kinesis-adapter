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

import software.amazon.keyspaces.streamsadapter.common.KeyspacesStreamsRequestsBuilder;
import software.amazon.keyspaces.streamsadapter.util.KinesisMapperUtil;
import software.amazon.keyspaces.streamsadapter.util.DescribeStreamResult;
import software.amazon.keyspaces.streamsadapter.util.ShardGraphTracker;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Synchronized;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.keyspacesstreams.model.GetStreamRequest;
import software.amazon.awssdk.services.keyspacesstreams.model.GetStreamResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.KinesisException;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;
import software.amazon.awssdk.services.kinesis.model.ResourceInUseException;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.utils.CollectionUtils;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.leases.ShardDetector;
import software.amazon.kinesis.retrieval.AWSExceptionManager;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Retrieves a Shard object from the cache based on the provided shardId.
 * <pre>
 * Start
 *   │
 *   ▼
 * Is Cache Empty? ──Yes──► Synchronized Block
 *   │                         │
 *   No                       ▼
 *   │                     Initialize Cache
 *   ▼
 * Get Shard from Cache
 *   │
 *   ▼
 * Shard Found? ──No──► Increment Cache Misses
 *   │                         │
 *   Yes                      ▼
 *   │               Need Refresh? ──Yes──► Synchronized Block
 *   │                         │                │
 *   │                        No               ▼
 *   │                         │            Refresh Cache
 *   │                         │                │
 *   │                         │                ▼
 *   │                         │            Reset Counter
 *   │                         │                │
 *   ▼                         ▼                ▼
 * Return Shard ◄────────────────────────────────
 * </pre>
 *
 */

@Slf4j
@Accessors(fluent = true)
public class KeyspacesStreamsShardDetector implements ShardDetector {

    @NonNull
    private final AmazonKeyspacesStreamsAdapterClient kinesisAsyncClient;

    @NonNull
    @Getter
    private final StreamIdentifier streamIdentifier;

    private final long listShardsCacheAllowedAgeInSeconds;
    private final int maxCacheMissesBeforeReload;
    private final int cacheMissWarningModulus;
    private final Duration kinesisRequestTimeout;

    private volatile Map<String, Shard> cachedShardMap = null;
    private volatile Instant lastCacheUpdateTime;

    @Getter(AccessLevel.PACKAGE)
    private final AtomicInteger cacheMisses = new AtomicInteger(0);

    private static final AWSExceptionManager AWS_EXCEPTION_MANAGER;

    static {
        AWS_EXCEPTION_MANAGER = new AWSExceptionManager();
        AWS_EXCEPTION_MANAGER.add(KinesisException.class, t -> t);
        AWS_EXCEPTION_MANAGER.add(LimitExceededException.class, t -> t);
        AWS_EXCEPTION_MANAGER.add(ResourceInUseException.class, t -> t);
        AWS_EXCEPTION_MANAGER.add(ResourceNotFoundException.class, t -> t);
    }

    public KeyspacesStreamsShardDetector(
            @NonNull AmazonKeyspacesStreamsAdapterClient kinesisAsyncClient,
            @NonNull StreamIdentifier streamIdentifier,
            long listShardsCacheAllowedAgeInSeconds,
            int maxCacheMissesBeforeReload,
            int cacheMissWarningModulus,
            Duration kinesisRequestTimeout) {
        this.kinesisAsyncClient = kinesisAsyncClient;
        this.streamIdentifier = streamIdentifier;
        this.listShardsCacheAllowedAgeInSeconds = listShardsCacheAllowedAgeInSeconds;
        this.maxCacheMissesBeforeReload = maxCacheMissesBeforeReload;
        this.cacheMissWarningModulus = cacheMissWarningModulus;
        this.kinesisRequestTimeout = kinesisRequestTimeout;
    }

    @Override
    public Shard shard(String shardId) {
        if (CollectionUtils.isNullOrEmpty(this.cachedShardMap)) {
            synchronized (this) {
                if (CollectionUtils.isNullOrEmpty(this.cachedShardMap)) {
                    listShards();
                }
            }
        }
        Shard shard = cachedShardMap.get(shardId);

        if (shard == null) {
            if (cacheMisses.incrementAndGet() > maxCacheMissesBeforeReload || shouldRefreshCache()) {
                synchronized (this) {
                    shard = cachedShardMap.get(shardId);

                    if (shard == null) {
                        log.info("Too many shard map cache misses or cache is out of date -- forcing a refresh");
                        describeStream();
                        shard = cachedShardMap.get(shardId);

                        if (shard == null) {
                            log.warn(
                                    "Even after cache refresh shard '{}' wasn't found. This could indicate a bigger"
                                            + " problem.",
                                    shardId);
                        }
                    }
                    cacheMisses.set(0);
                }
            }
        }

        if (shard == null) {
            final String message =
                    String.format("Cannot find the shard given the shardId %s. Cache misses: %s", shardId, cacheMisses);
            if (cacheMisses.get() % cacheMissWarningModulus == 0) {
                log.warn(message);
            } else {
                log.debug(message);
            }
        }

        return shard;
    }

    @Override
    @Synchronized
    public List<Shard> listShards() {
        DescribeStreamResult describeStreamResult = describeStream();
        return describeStreamResult.getShards();
    }

    @Synchronized
    public DescribeStreamResult describeStream() {
        ShardGraphTracker shardTracker = new ShardGraphTracker();
        DescribeStreamResult describeStreamResult = new DescribeStreamResult();
        GetStreamResponse getStreamResponse;
        String nextToken = null;

        // Phase 1: Collect all shards from Paginations.
        do {
            getStreamResponse = getStreamResponse(nextToken);

            // Convert Keyspaces shards to Kinesis shards and collect them
            List<Shard> kinesisShards = getStreamResponse.shards().stream()
                    .map(KinesisMapperUtil::convertKeyspacesShardToKinesisShard)
                    .collect(Collectors.toList());

            shardTracker.collectShards(kinesisShards);

            describeStreamResult.addStatus(getStreamResponse.streamStatusAsString());
            nextToken = getStreamResponse.nextToken();
        } while (nextToken != null);

        // Phase 2: Close open parents
        shardTracker.closeOpenParents();

        // Phase 3: Mark leaf shards as active if stream is disabled
        if (Objects.equals(describeStreamResult.getStreamStatus(), "DISABLED")) {
            shardTracker.markLeafShardsActive();
        }

        // Get processed shards and update result
        List<Shard> processedShards = shardTracker.getShards();
        describeStreamResult.addShards(processedShards);

        // Update cache
        cachedShardMap(processedShards);

        return describeStreamResult;
    }

    private GetStreamResponse getStreamResponse(String nextToken) {
        String streamArn = KinesisMapperUtil.createKeyspacesStreamsArnFromKinesisStreamName(this.streamIdentifier.streamName());
        GetStreamRequest getStreamRequest = KeyspacesStreamsRequestsBuilder.getStreamRequestBuilder()
                .streamArn(streamArn)
                .nextToken(nextToken)
                .build();

        GetStreamResponse getStreamResponse;
        try {
            getStreamResponse = this.kinesisAsyncClient.getStream(getStreamRequest).get();
        } catch (ExecutionException e) {
            throw AWS_EXCEPTION_MANAGER.apply(e.getCause());
        } catch (InterruptedException e) {
            log.debug("Interrupted exception caught, shutdown initiated, returning null");
            return null;
        }

        if (getStreamResponse == null) {
            throw new IllegalStateException("Received null from GetStream call.");
        }

        return getStreamResponse;
    }

    private String getNextTokenFromResponse(DescribeStreamResponse response) {
        List<Shard> shards = response.streamDescription().shards();
        if (!shards.isEmpty()) {
            return shards.get(shards.size() - 1).shardId();
        }
        return null;
    }

    private boolean shouldRefreshCache() {
        final Duration secondsSinceLastUpdate = Duration.between(lastCacheUpdateTime, Instant.now());
        final String message = String.format("Shard map cache is %d seconds old", secondsSinceLastUpdate.getSeconds());
        if (secondsSinceLastUpdate.compareTo(Duration.of(listShardsCacheAllowedAgeInSeconds, ChronoUnit.SECONDS)) > 0) {
            log.info("{}. Age exceeds limit of {} seconds -- Refreshing.", message, listShardsCacheAllowedAgeInSeconds);
            return true;
        }

        log.debug("{}. Age doesn't exceed limit of {} seconds.", message, listShardsCacheAllowedAgeInSeconds);
        return false;
    }

    private void cachedShardMap(final List<Shard> shards) {
        cachedShardMap = shards.stream().collect(Collectors.toMap(Shard::shardId, Function.identity()));
        lastCacheUpdateTime = Instant.now();
    }
}