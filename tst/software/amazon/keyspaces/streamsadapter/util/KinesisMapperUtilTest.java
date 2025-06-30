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

import org.junit.jupiter.api.Test;

import software.amazon.awssdk.services.keyspacesstreams.model.SequenceNumberRange;
import software.amazon.awssdk.services.kinesis.model.Shard;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KinesisMapperUtilTest {

    @Test
    void testConvertKeyspacesShardToKinesisShard() {
        // Arrange
        List<String> parentShardIds = new ArrayList<>();
        parentShardIds.add("parentShardId-123");

        software.amazon.awssdk.services.keyspacesstreams.model.Shard keyspacesShard =
                software.amazon.awssdk.services.keyspacesstreams.model.Shard.builder()
                        .shardId("shardId-123")
                        .parentShardIds(parentShardIds)
                        .sequenceNumberRange(SequenceNumberRange.builder()
                                .startingSequenceNumber("100")
                                .endingSequenceNumber("200")
                                .build())
                        .build();

        // Act
        Shard kinesisShard = KinesisMapperUtil.convertKeyspacesShardToKinesisShard(keyspacesShard);

        // Assert
        assertEquals("shardId-123", kinesisShard.shardId());
        assertEquals("parentShardId-123", kinesisShard.parentShardId());
        assertNull(kinesisShard.adjacentParentShardId());
        assertEquals("100", kinesisShard.sequenceNumberRange().startingSequenceNumber());
        assertEquals("200", kinesisShard.sequenceNumberRange().endingSequenceNumber());
        assertEquals("0", kinesisShard.hashKeyRange().startingHashKey());
        assertEquals("1", kinesisShard.hashKeyRange().endingHashKey());
    }

    @Test
    void testCreateKinesisStreamNameSingleStreamFormat() {
        // Test ARN with special characters in table name
        String arnWithSpecialChars = "arn:aws:cassandra:us-west-2:123456789012:/keyspace/test_keyspace/table/Test-Table_123/stream/2024-02-03T00:00:00.000";
        String result = KinesisMapperUtil.createKinesisStreamIdentifierFromKeyspacesStreamsArn(arnWithSpecialChars, false);
        assertEquals("us-west-2$123456789012$test_keyspace$Test-Table_123$2024-02-03T00_00_00.000", result);
    }

    @Test
    void testCreateKinesisStreamNameMultiStreamFormat() {
        // Test multi-stream format
        String arn = "arn:aws:cassandra:us-west-2:123456789012:/keyspace/test_keyspace/table/TestTable/stream/2024-02-03T00:00:00.000";
        String result = KinesisMapperUtil.createKinesisStreamIdentifierFromKeyspacesStreamsArn(arn, true);
        assertEquals("123456789012:us-west-2$123456789012$test_keyspace$TestTable$2024-02-03T00_00_00.000:1", result);

        // Test multi-stream format with special characters
        String arnWithSpecialChars = "arn:aws:cassandra:us-west-2:123456789012:/keyspace/test_keyspace/table/Test-Table_123/stream/2024-02-03T00:00:00.000";
        String specialResult = KinesisMapperUtil.createKinesisStreamIdentifierFromKeyspacesStreamsArn(arnWithSpecialChars, true);
        assertEquals("123456789012:us-west-2$123456789012$test_keyspace$Test-Table_123$2024-02-03T00_00_00.000:1", specialResult);
    }

    @Test
    void testCreateKeyspacesStreamsArnEdgeCases() {
        // Test with missing components
        String invalidFormat = "region$account";
        assertThrows(ArrayIndexOutOfBoundsException.class, () ->
                KinesisMapperUtil.createKeyspacesStreamsArnFromKinesisStreamName(invalidFormat));

        // Test with empty components
        String emptyComponents = "region$$$$$";
        assertThrows(ArrayIndexOutOfBoundsException.class, () ->
                KinesisMapperUtil.createKeyspacesStreamsArnFromKinesisStreamName(emptyComponents));
    }

    @Test
    void testSingleStreamIdentifierConsistency() {
        // Test that stream identifiers remain consistent through multiple conversions
        String originalArn = "arn:aws:cassandra:us-west-2:123456789012:/keyspace/test_keyspace/table/TestTable/stream/2024-02-03T00:00:00.000";
        String streamIdentifier = KinesisMapperUtil.createKinesisStreamIdentifierFromKeyspacesStreamsArn(originalArn, false);
        String reconstructedArn = KinesisMapperUtil.createKeyspacesStreamsArnFromKinesisStreamName(streamIdentifier);

        assertEquals(originalArn, reconstructedArn);
    }

    @Test
    void testMultiStreamIdentifierConsistency() {
        // Test that stream identifiers remain consistent through multiple conversions
        String originalArn = "arn:aws:cassandra:us-west-2:123456789012:/keyspace/test_keyspace/table/TestTable/stream/2024-02-03T00:00:00.000";
        String streamIdentifier = KinesisMapperUtil.createKinesisStreamIdentifierFromKeyspacesStreamsArn(originalArn, true);
        // Extract the middle part from multi-stream format
        String middlePart = streamIdentifier.split(":")[1];
        String reconstructedArn = KinesisMapperUtil.createKeyspacesStreamsArnFromKinesisStreamName(middlePart);

        assertEquals(originalArn, reconstructedArn);
    }

    @Test
    void testValidateArnFormat() {
        // Valid ARNs
        assertTrue(KinesisMapperUtil.isValidKeyspacesStreamArn(
                "arn:aws:cassandra:us-west-2:123456789012:/keyspace/test_keyspace/table/TestTable/stream/2024-02-03T00:00:00.000"));

        // Invalid ARNs
        assertFalse(KinesisMapperUtil.isValidKeyspacesStreamArn(null));
        assertFalse(KinesisMapperUtil.isValidKeyspacesStreamArn(""));
        assertFalse(KinesisMapperUtil.isValidKeyspacesStreamArn("arn:aws:cassandra:region:account:/keyspace/name")); // missing table and stream
        assertFalse(KinesisMapperUtil.isValidKeyspacesStreamArn("arn:aws:dynamodb:region:account:table/name/stream/label")); // wrong service
    }

    @Test
    void testTableNameWithSpecialCharacters() {
        // Test various special characters in table names
        String[] specialTableNames = {
                "Table-Name",
                "Table_Name",
                "Table123",
                "123Table",
                "Table-123_Test"
        };

        for (String tableName : specialTableNames) {
            String arn = String.format("arn:aws:cassandra:us-west-2:123456789012:/keyspace/test_keyspace/table/%s/stream/2024-02-03T00:00:00.000",
                    tableName);
            String result = KinesisMapperUtil.createKinesisStreamIdentifierFromKeyspacesStreamsArn(arn, false);
            String reconstructedArn = KinesisMapperUtil.createKeyspacesStreamsArnFromKinesisStreamName(result);
            assertEquals(arn, reconstructedArn);
        }
    }

    @Test
    void testStreamLabelFormats() {
        // Test various stream label formats
        String[] streamLabels = {
                "2024-02-03T00:00:00.000",
                "2024-02-03T00:00:00.999",
                "1970-01-01T00:00:00.000",
                "9999-12-31T23:59:59.999"
        };

        for (String label : streamLabels) {
            String arn = String.format("arn:aws:cassandra:us-west-2:123456789012:/keyspace/test_keyspace/table/TestTable/stream/%s",
                    label);
            assertTrue(KinesisMapperUtil.isValidKeyspacesStreamArn(arn));
        }
    }

    @Test
    void testRoundTripConversionWithMultiStream() {
        // Test that converting to multi-stream format and back works correctly
        String originalArn = "arn:aws:cassandra:us-west-2:123456789012:/keyspace/test_keyspace/table/TestTable/stream/2024:02:03T00:00:00.000";
        String multiStreamFormat = KinesisMapperUtil.createKinesisStreamIdentifierFromKeyspacesStreamsArn(originalArn, true);
        String recreatedArn = KinesisMapperUtil.createKeyspacesStreamsArnFromKinesisStreamName(multiStreamFormat);
        assertEquals(originalArn, recreatedArn);
    }

    @Test
    void testRoundTripConversionWithSingleStream() {
        // Test that converting to single-stream format and back works correctly
        String originalArn = "arn:aws:cassandra:us-west-2:123456789012:/keyspace/test_keyspace/table/TestTable/stream/2024:02:03T00:00:00.000";
        String singleStreamFormat = KinesisMapperUtil.createKinesisStreamIdentifierFromKeyspacesStreamsArn(originalArn, false);
        String recreatedArn = KinesisMapperUtil.createKeyspacesStreamsArnFromKinesisStreamName(singleStreamFormat);
        assertEquals(originalArn, recreatedArn);
    }

    @Test
    void testCreateKinesisStreamIdentifierWithSpecialCharactersInTableName() {
        String streamArn = "arn:aws:cassandra:us-west-2:123456789012:/keyspace/test_keyspace/table/Test-Table_123/stream/2024-02-03T00:00:00.000";
        String result = KinesisMapperUtil.createKinesisStreamIdentifierFromKeyspacesStreamsArn(streamArn, false);

        // Verify special characters are preserved
        assertTrue(result.contains("Test-Table_123"));

        // Verify round trip conversion
        String reconstructedArn = KinesisMapperUtil.createKeyspacesStreamsArnFromKinesisStreamName(result);
        assertEquals(streamArn, reconstructedArn);
    }

    @Test
    void testCreateKinesisStreamIdentifierWithInvalidArn() {
        String invalidArn = "arn:aws:cassandra:us-west-2:123456789012:/keyspace//table/TestTable/stream/2024-02-03T00:00:00.000";

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> KinesisMapperUtil.createKinesisStreamIdentifierFromKeyspacesStreamsArn(invalidArn, false)
        );

        assertTrue(exception.getMessage().contains("Invalid Keyspaces stream ARN"));
    }

    @Test
    void testCreateKeyspacesStreamsArnWithInvalidFormat() {
        String invalidStreamName = "invalid$format$missing$components";

        assertThrows(
                ArrayIndexOutOfBoundsException.class,
                () -> KinesisMapperUtil.createKeyspacesStreamsArnFromKinesisStreamName(invalidStreamName)
        );
    }

    @Test
    void testStreamIdentifierWithMaxLengthValues() {
        // Create ARN with maximum allowed lengths for components
        String region = "us-west-2";
        String accountId = "123456789012";
        String keyspaceName = "test_keyspace";
        String tableName = String.join("", Collections.nCopies(48, "a")); // Max Keyspaces table name length
        String timestamp = "2024-02-03T00:00:00.000";

        String streamArn = String.format("arn:aws:cassandra:%s:%s:/keyspace/%s/table/%s/stream/%s",
                region, accountId, keyspaceName, tableName, timestamp);

        // Test both single and multi-stream modes
        String singleStreamId = KinesisMapperUtil.createKinesisStreamIdentifierFromKeyspacesStreamsArn(streamArn, false);
        String multiStreamId = KinesisMapperUtil.createKinesisStreamIdentifierFromKeyspacesStreamsArn(streamArn, true);

        assertNotNull(singleStreamId);
        assertNotNull(multiStreamId);

        // Verify round-trip conversion works for both
        assertEquals(streamArn, KinesisMapperUtil.createKeyspacesStreamsArnFromKinesisStreamName(singleStreamId));
        assertTrue(multiStreamId.startsWith(accountId + ":"));
    }

    @Test
    void testGetShardCreationTime() {
        String shardId = "shardId-1234567890";
        Instant creationTime = KinesisMapperUtil.getShardCreationTime(shardId);
        assertNotNull(creationTime);
        assertEquals(1234567890, creationTime.toEpochMilli());
    }

    @Test
    void testKeyspacesStreamArnWithPartition() {
        // Test ARN with different AWS partition
        String arnWithPartition = "arn:aws-cn:cassandra:cn-north-1:123456789012:/keyspace/test_keyspace/table/TestTable/stream/2024-02-03T00:00:00.000";
        assertTrue(KinesisMapperUtil.isValidKeyspacesStreamArn(arnWithPartition));

        String result = KinesisMapperUtil.createKinesisStreamIdentifierFromKeyspacesStreamsArn(arnWithPartition, false);
        String reconstructedArn = KinesisMapperUtil.createKeyspacesStreamsArnFromKinesisStreamName(result);
        assertEquals(arnWithPartition, reconstructedArn);
    }

    @Test
    void testKeyspacesStreamArnWithSpecialCharactersInKeyspaceName() {
        String streamArn = "arn:aws:cassandra:us-west-2:123456789012:/keyspace/test_keyspace_123/table/TestTable/stream/2024-02-03T00:00:00.000";
        String result = KinesisMapperUtil.createKinesisStreamIdentifierFromKeyspacesStreamsArn(streamArn, false);

        // Verify special characters are preserved
        assertTrue(result.contains("test_keyspace_123"));

        // Verify round trip conversion
        String reconstructedArn = KinesisMapperUtil.createKeyspacesStreamsArnFromKinesisStreamName(result);
        assertEquals(streamArn, reconstructedArn);
    }
}
