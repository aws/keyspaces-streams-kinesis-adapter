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

package software.amazon.keyspaces.streamsadapter.serialization;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesCell;
import software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesCellMapDefinition;
import software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesCellValue;
import software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesMetadata;
import software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesRow;
import software.amazon.awssdk.services.keyspacesstreams.model.Record;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RecordObjectMapperTest {
    private static final ObjectMapper MAPPER = new RecordObjectMapper();

    private static final String TEST_EVENT_VERSION = "1.1";
    private static final String TEST_ORIGIN = "USER";
    private static final String TEST_WRITE_TIME = "1234567890";
    private static final String TEST_EXPIRATION_TIME = "1234567899";
    private static final String TEST_CELL_VALUE = "test_value";
    private static final String TEST_COLUMN_NAME = "test_col";
    private static final String PARTITION_KEY = "partition_key";
    private static final String CLUSTERING_KEY = "clustering_key";
    private static final String PK_KEY = "pk";
    private static final String CK_KEY = "ck";
    private static final String TEST_STRING = "TestString";
    private static final Date TEST_DATE = new Date(1156377600 /* EC2 Announced */);

    private Record testRecord;

    @BeforeEach
    public void setUpTest() {
        KeyspacesCellValue pk = KeyspacesCellValue.builder().asciiT(PARTITION_KEY).build();
        Map<String, KeyspacesCellValue> partitionKeys = new HashMap<>();
        partitionKeys.put(PK_KEY, pk);

        KeyspacesCellValue ck = KeyspacesCellValue.builder().asciiT(CLUSTERING_KEY).build();
        Map<String, KeyspacesCellValue> clusteringKeys = new HashMap<>();
        clusteringKeys.put(CK_KEY, ck);

        testRecord = Record.builder()
                .eventVersion(TEST_EVENT_VERSION)
                .createdAt(TEST_DATE.toInstant())
                .origin(TEST_ORIGIN)
                .partitionKeys(partitionKeys)
                .clusteringKeys(clusteringKeys)
                .sequenceNumber(TEST_STRING)
                .build();
    }

    private void valiateSerializedRecord(String expectedJsonPath, Record record) throws IOException {
        // Read expected JSON from file
        String expectedJSON = new String(java.nio.file.Files.readAllBytes(
                java.nio.file.Paths.get(expectedJsonPath)));

        // Remove whitespace for comparison (since the file has formatting)
        ObjectMapper plainMapper = new ObjectMapper();
        JsonNode expectedNode = plainMapper.readTree(expectedJSON);
        JsonNode actualNode = plainMapper.readTree(MAPPER.writeValueAsString(record));

        Assertions.assertEquals(expectedNode, actualNode);
    }

    @Test
    public void testRecordWithRowDataAndMetadata() throws IOException {
        // Create a cell with metadata
        KeyspacesCell cellWithMetadata = KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().textT(TEST_CELL_VALUE).build())
                .metadata(KeyspacesMetadata.builder()
                        .writeTime(TEST_WRITE_TIME)
                        .expirationTime(TEST_EXPIRATION_TIME)
                        .build())
                .build();

        Map<String, KeyspacesCell> valueCells = new HashMap<>();
        valueCells.put(TEST_COLUMN_NAME, cellWithMetadata);

        Record record = testRecord.toBuilder()
                .newImage(KeyspacesRow.builder().valueCells(valueCells).build())
                .oldImage((KeyspacesRow) null)
                .build();

        valiateSerializedRecord("tst/resources/simple_record_expected.json", record);
    }

    @Test
    public void testAllScalarTypes() throws IOException {
        Map<String, KeyspacesCell> valueCells = new HashMap<>();

        // Add all scalar types
        valueCells.put("ascii_col", KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().asciiT("test_ascii").build())
                .build());

        valueCells.put("bigint_col", KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().bigintT("9223372036854775807").build())
                .build());

        byte[] blobBytes = new byte[]{0, 0, 0, 1};
        valueCells.put("blob_col", KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder()
                        .blobT(SdkBytes.fromByteBuffer(ByteBuffer.wrap(blobBytes)))
                        .build())
                .build());

        valueCells.put("bool_col", KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().boolT(true).build())
                .build());

        valueCells.put("date_col", KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().dateT("2024-01-01").build())
                .build());

        valueCells.put("decimal_col", KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().decimalT("123.456").build())
                .build());

        valueCells.put("double_col", KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().doubleT("1.7976").build())
                .build());

        valueCells.put("float_col", KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().floatT("3.4028235E38").build())
                .build());

        valueCells.put("inet_col", KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().inetT("192.168.1.1").build())
                .build());

        valueCells.put("int_col", KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().intT("2147483647").build())
                .build());

        valueCells.put("smallint_col", KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().smallintT("32767").build())
                .build());

        valueCells.put("text_col", KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().textT("test_text").build())
                .build());

        valueCells.put("time_col", KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().timeT("12:34:56.789").build())
                .build());

        valueCells.put("timeuuid_col", KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().timeuuidT("00000000-0000-0000-0000-000000000000").build())
                .build());

        valueCells.put("tinyint_col", KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().tinyintT("127").build())
                .build());

        valueCells.put("uuid_col", KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().uuidT("00000000-0000-0000-0000-000000000000").build())
                .build());

        valueCells.put("varchar_col", KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().varcharT("test_varchar").build())
                .build());

        valueCells.put("varint_col", KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().varintT("1234567890").build())
                .build());

        valueCells.put("counter_col", KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().counterT("42").build())
                .build());

        Record record = testRecord.toBuilder()
                .newImage(KeyspacesRow.builder().valueCells(valueCells).build())
                .oldImage((KeyspacesRow) null)
                .build();

        valiateSerializedRecord("tst/resources/scalar_record_expected.json", record);
    }

    @Test
    public void testCollectionTypes() throws IOException {
        Map<String, KeyspacesCell> valueCells = new HashMap<>();

        // List
        List<KeyspacesCell> list = Arrays.asList(
                KeyspacesCell.builder()
                        .value(KeyspacesCellValue.builder().textT("list_item1").build())
                        .build(),
                KeyspacesCell.builder()
                        .value(KeyspacesCellValue.builder().textT("list_item2").build())
                        .build()
        );
        valueCells.put("list_col", KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().listT(list).build())
                .build());

        // Set
        List<KeyspacesCell> set = Arrays.asList(
                KeyspacesCell.builder()
                        .value(KeyspacesCellValue.builder().textT("set_item1").build())
                        .build(),
                KeyspacesCell.builder()
                        .value(KeyspacesCellValue.builder().textT("set_item2").build())
                        .build()
        );
        valueCells.put("set_col", KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().setT(set).build())
                .build());

        // Map
        List<KeyspacesCellMapDefinition> map = new ArrayList<>();
        map.add(KeyspacesCellMapDefinition.builder()
                .key(KeyspacesCellValue.builder().textT("key1").build())
                .value(KeyspacesCellValue.builder().textT("value1").build())
                .build());
        map.add(KeyspacesCellMapDefinition.builder()
                .key(KeyspacesCellValue.builder().textT("key2").build())
                .value(KeyspacesCellValue.builder().textT("value2").build())
                .build());
        valueCells.put("map_col", KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().mapT(map).build())
                .build());

        // UDT
        Map<String, KeyspacesCell> udtFields = new HashMap<>();
        udtFields.put("field1", KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().textT("udt_value1").build())
                .build());
        udtFields.put("field2", KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().textT("udt_value2").build())
                .build());
        valueCells.put("udt_col", KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().udtT(udtFields).build())
                .build());

        // Tuple
        List<KeyspacesCell> tuple = Arrays.asList(
                KeyspacesCell.builder()
                        .value(KeyspacesCellValue.builder().textT("tuple_item1").build())
                        .build(),
                KeyspacesCell.builder()
                        .value(KeyspacesCellValue.builder().intT("123").build())
                        .build()
        );
        valueCells.put("tuple_col", KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().tupleT(tuple).build())
                .build());

        Record record = testRecord.toBuilder()
                .newImage(KeyspacesRow.builder().valueCells(valueCells).build())
                .oldImage((KeyspacesRow) null)
                .build();

        valiateSerializedRecord("tst/resources/collections_record_expected.json", record);
    }

    @Test
    public void testEmptyCollections() throws IOException {
        Map<String, KeyspacesCell> valueCells = new HashMap<>();

        // Empty list
        valueCells.put("empty_list", KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().listT(Collections.emptyList()).build())
                .build());

        // Empty set
        valueCells.put("empty_set", KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().setT(Collections.emptyList()).build())
                .build());

        // Empty map
        valueCells.put("empty_map", KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().mapT(Collections.emptyList()).build())
                .build());

        // Empty UDT
        valueCells.put("empty_udt", KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().udtT(Collections.emptyMap()).build())
                .build());

        // Empty tuple
        valueCells.put("empty_tuple", KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().tupleT(Collections.emptyList()).build())
                .build());

        Record record = testRecord.toBuilder()
                .newImage(KeyspacesRow.builder().valueCells(valueCells).build())
                .build();

        valiateSerializedRecord("tst/resources/empty_collections_record_expected.json", record);
    }

    @Test
    public void testMaxDepthNestedCollections() throws IOException {
        Map<String, KeyspacesCell> valueCells = new HashMap<>();

        // Level 5 (innermost) - Set of strings
        List<KeyspacesCell> innerSet = Arrays.asList(
                KeyspacesCell.builder()
                        .value(KeyspacesCellValue.builder().textT("leaf1").build())
                        .build(),
                KeyspacesCell.builder()
                        .value(KeyspacesCellValue.builder().textT("leaf2").build())
                        .build()
        );
        KeyspacesCell innerSetCell = KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().setT(innerSet).build())
                .build();

        // Level 4 - Map containing sets (2 entries)
        KeyspacesCellMapDefinition mapEntry1 = KeyspacesCellMapDefinition.builder()
                .key(KeyspacesCellValue.builder().textT("level4key1").build())
                .value(innerSetCell.value())
                .build();
        KeyspacesCellMapDefinition mapEntry2 = KeyspacesCellMapDefinition.builder()
                .key(KeyspacesCellValue.builder().textT("level4key2").build())
                .value(innerSetCell.value())
                .build();
        KeyspacesCell mapCell = KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder()
                        .mapT(Arrays.asList(mapEntry1, mapEntry2))
                        .build())
                .build();

        // Level 3 - List containing single map
        KeyspacesCell listCell = KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder()
                        .listT(Collections.singletonList(mapCell))
                        .build())
                .build();

        // Level 2 - Set containing single list
        KeyspacesCell outerSetCell = KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder()
                        .setT(Collections.singletonList(listCell))
                        .build())
                .build();

        // Level 1 (outermost) - List containing single set
        KeyspacesCell maxDepthCell = KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder()
                        .listT(Collections.singletonList(outerSetCell))
                        .build())
                .build();

        valueCells.put("max_depth_nested", maxDepthCell);

        Record record = testRecord.toBuilder()
                .newImage(KeyspacesRow.builder().valueCells(valueCells).build())
                .oldImage((KeyspacesRow) null)
                .build();

        valiateSerializedRecord("tst/resources/max_depth_collections_record_expected.json", record);
    }

    @Test
    public void testMaxDepthNestedUdt() throws IOException {
        Map<String, KeyspacesCell> valueCells = new HashMap<>();

        // Start with the deepest level (level 8)
        Map<String, KeyspacesCell> level8Fields = new HashMap<>();
        level8Fields.put("leaf", KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().textT("deepest_value").build())
                .build());

        KeyspacesCell level8Udt = KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().udtT(level8Fields).build())
                .build();

        // Level 7
        Map<String, KeyspacesCell> level7Fields = new HashMap<>();
        level7Fields.put("level8", level8Udt);
        level7Fields.put("field7", KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().textT("value_7").build())
                .build());

        KeyspacesCell level7Udt = KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().udtT(level7Fields).build())
                .build();

        // Level 6
        Map<String, KeyspacesCell> level6Fields = new HashMap<>();
        level6Fields.put("level7", level7Udt);
        level6Fields.put("field6", KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().textT("value_6").build())
                .build());

        KeyspacesCell level6Udt = KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().udtT(level6Fields).build())
                .build();

        // Level 5
        Map<String, KeyspacesCell> level5Fields = new HashMap<>();
        level5Fields.put("level6", level6Udt);
        level5Fields.put("field5", KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().textT("value_5").build())
                .build());

        KeyspacesCell level5Udt = KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().udtT(level5Fields).build())
                .build();

        // Level 4
        Map<String, KeyspacesCell> level4Fields = new HashMap<>();
        level4Fields.put("level5", level5Udt);
        level4Fields.put("field4", KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().textT("value_4").build())
                .build());

        KeyspacesCell level4Udt = KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().udtT(level4Fields).build())
                .build();

        // Level 3
        Map<String, KeyspacesCell> level3Fields = new HashMap<>();
        level3Fields.put("level4", level4Udt);
        level3Fields.put("field3", KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().textT("value_3").build())
                .build());

        KeyspacesCell level3Udt = KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().udtT(level3Fields).build())
                .build();

        // Level 2
        Map<String, KeyspacesCell> level2Fields = new HashMap<>();
        level2Fields.put("level3", level3Udt);
        level2Fields.put("field2", KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().textT("value_2").build())
                .build());

        KeyspacesCell level2Udt = KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().udtT(level2Fields).build())
                .build();

        // Level 1 (outermost)
        Map<String, KeyspacesCell> level1Fields = new HashMap<>();
        level1Fields.put("level2", level2Udt);
        level1Fields.put("top_level_field", KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().textT("root_value").build())
                .build());

        KeyspacesCell maxDepthUdtCell = KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().udtT(level1Fields).build())
                .build();

        valueCells.put("max_depth_udt", maxDepthUdtCell);

        Record record = testRecord.toBuilder()
                .newImage(KeyspacesRow.builder().valueCells(valueCells).build())
                .oldImage((KeyspacesRow) null)
                .build();

        valiateSerializedRecord("tst/resources/max_depth_udt_record_expected.json", record);
    }

    @Test
    public void testMetadataAtMultipleLevels() throws IOException {
        Map<String, KeyspacesCell> valueCells = new HashMap<>();

        // Simple text field with metadata
        KeyspacesCell textCell = KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().textT("simple_text_value").build())
                .metadata(KeyspacesMetadata.builder()
                        .writeTime("1234567890")
                        .expirationTime("1234567899")
                        .build())
                .build();
        valueCells.put("text_field", textCell);

        // List with metadata at collection and element level
        List<KeyspacesCell> listWithMetadata = Arrays.asList(
                KeyspacesCell.builder()
                        .value(KeyspacesCellValue.builder().textT("list_item1").build())
                        .metadata(KeyspacesMetadata.builder()
                                .writeTime("1234567891")
                                .expirationTime("1234567900")
                                .build())
                        .build(),
                KeyspacesCell.builder()
                        .value(KeyspacesCellValue.builder().textT("list_item2").build())
                        .metadata(KeyspacesMetadata.builder()
                                .writeTime("1234567892")
                                .expirationTime("1234567901")
                                .build())
                        .build()
        );

        KeyspacesCell listCell = KeyspacesCell.builder()
                .value(KeyspacesCellValue.builder().listT(listWithMetadata).build())
                .metadata(KeyspacesMetadata.builder()
                        .writeTime("1234567893")
                        .expirationTime("1234567902")
                        .build())
                .build();
        valueCells.put("list_field", listCell);

        // Create record with row-level metadata
        Record record = testRecord.toBuilder()
                .newImage(KeyspacesRow.builder()
                        .valueCells(valueCells)
                        .rowMetadata(KeyspacesMetadata.builder()
                                .writeTime("1234567894")
                                .expirationTime("1234567903")
                                .build())
                        .build())
                .oldImage((KeyspacesRow) null)
                .build();

        valiateSerializedRecord("tst/resources/atso_and_ttl_record_expected.json", record);
    }

}