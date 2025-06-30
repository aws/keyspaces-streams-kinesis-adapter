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

package software.amazon.keyspaces.streamsadapter.util;

import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.kinesis.model.HashKeyRange;
import software.amazon.awssdk.services.kinesis.model.SequenceNumberRange;
import software.amazon.awssdk.services.kinesis.model.Shard;
import java.math.BigInteger;
import java.time.Instant;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class KinesisMapperUtil {
    /**
     * Pattern for a Keyspaces stream ARN. The valid format is
     * {@code arn:aws:cassandra:<region>:<accountId>:/keyspace/<keyspaceName>/table/<tableName>/stream/<streamLabel>}
     */
    private static final Pattern KEYSPACES_STREAM_ARN_PATTERN = Pattern.compile(
            "arn:aws(?:-cn)?:cassandra:(?<region>[-a-z0-9]+):(?<accountId>[0-9]{12}):/keyspace/(?<keyspaceName>[^/]+)/table/(?<tableName>[^/]+)/stream/(?<streamLabel>.+)");

    private static final String DELIMITER = "$";
    private static final String COLON_REPLACEMENT = "_";
    private static final String SHARD_ID_SEPARATOR = "-";

    public static final Duration MIN_LEASE_RETENTION_DURATION_IN_HOURS = Duration.ofHours(6);

    /**
     * Converts a Keyspaces Streams Shard to a Kinesis Shard
     * @return {@link Shard} kinesisShard
     *
     */
    public static Shard convertKeyspacesShardToKinesisShard(software.amazon.awssdk.services.keyspacesstreams.model.Shard keyspacesShard) {
        String parentShardId = null;
        if (keyspacesShard.parentShardIds() != null && !keyspacesShard.parentShardIds().isEmpty()) {
            parentShardId = keyspacesShard.parentShardIds().get(0);
        }
        return Shard.builder()
                .shardId(keyspacesShard.shardId())
                .parentShardId(parentShardId)
                .adjacentParentShardId(null)
                .sequenceNumberRange(SequenceNumberRange.builder()
                        .startingSequenceNumber(keyspacesShard.sequenceNumberRange().startingSequenceNumber())
                        .endingSequenceNumber(keyspacesShard.sequenceNumberRange().endingSequenceNumber())
                        .build()
                )
                // set the hash key range from 0-1 to ensure periodic shard sync is always run
                .hashKeyRange(HashKeyRange.builder()
                        .startingHashKey(BigInteger.ZERO.toString())
                        .endingHashKey(BigInteger.ONE.toString())
                        .build()
                )
                .build();
    }

    /**
     * This method extracts the shard creation time from the ShardId
     *
     * @param shardId
     * @return instant at which the shard was created
     */
    public static Instant getShardCreationTime(String shardId) {
        return Instant.ofEpochMilli(Long.parseLong(shardId.split(SHARD_ID_SEPARATOR)[1]));
    }

    /**
     * Validates if the given string is a valid Keyspaces Stream ARN
     */
    public static boolean isValidKeyspacesStreamArn(String arn) {
        if (arn == null) {
            return false;
        }
        return KEYSPACES_STREAM_ARN_PATTERN.matcher(arn).matches();
    }

    /**
     * Creates a Kinesis-format StreamIdentifier from a Keyspaces Stream ARN
     * Converts stream label from colon to underscore to avoid issues with colon in shardId
     * Format: region$accountId$tableName$underscore_separated_streamLabel for single streaming case
     * Format: accountId:region$accountId$tableName$underscore_separated_streamLabel:1 for multi-streaming case
     * @return {@link String} streamIdentifier
     */
    public static String createKinesisStreamIdentifierFromKeyspacesStreamsArn(String keyspacesStreamArn, boolean isMultiStreamMode) {
        Matcher matcher = KEYSPACES_STREAM_ARN_PATTERN.matcher(keyspacesStreamArn);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid Keyspaces stream ARN format: " + keyspacesStreamArn);
        }
        String region = matcher.group("region");
        String accountId = matcher.group("accountId");
        String keyspaceName = matcher.group("keyspaceName");
        String tableName = matcher.group("tableName");
        String streamLabel = matcher.group("streamLabel").replace(":", COLON_REPLACEMENT);
        // Create a unique stream name that can be used for lease keys
        String kinesisStreamName = String.join(DELIMITER, region, accountId, keyspaceName, tableName, streamLabel);

        if (isMultiStreamMode) {
            return String.join(":", accountId, kinesisStreamName, "1");
        }
        return kinesisStreamName;
    }

    /**
     * Converts the stream name created by {@link #createKinesisStreamIdentifierFromKeyspacesStreamsArn(String, boolean)}}
     * to Keyspaces stream ARN
     * @return {@link String} keypsacesStreamArn
     */
    public static String createKeyspacesStreamsArnFromKinesisStreamName(String streamName) {
        boolean isMultiStreamMode = streamName.contains(":");

        String streamNameToUse = isMultiStreamMode ? streamName.split(":")[1] : streamName;
        String[] parts = streamNameToUse.split(Pattern.quote(DELIMITER));
        String region = parts[0];
        String accountId = parts[1];
        String keyspaceName = parts[2];
        String tableName = parts[3];
        String streamLabel = parts[4].replace(COLON_REPLACEMENT, ":");

        // Determine partition based on region
        String partition = region.startsWith("cn-") ? "aws-cn" : "aws";

        String keyspacesStreamArn = String.format("arn:%s:cassandra:%s:%s:/keyspace/%s/table/%s/stream/%s",
                partition, region, accountId, keyspaceName, tableName, streamLabel);
        if (!isValidKeyspacesStreamArn(keyspacesStreamArn)) {
            throw new IllegalArgumentException("Invalid Keyspaces stream ARN: " + keyspacesStreamArn);
        }
        return keyspacesStreamArn;
    }
}
