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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesCellValue;

/**
 * Custom serializer for KeyspacesCellValues.
 * This serializer is needed because KeyspacesCellValue uses AWS SDK's auto-constructed collections by default.
 *    We need to distinguish between:
 *    - Default (auto-constructed) collections which should not be serialized
 *    - Explicitly set collections (even if empty) which should be serialized
 */
public class KeyspacesCellValueSerializer extends JsonSerializer<KeyspacesCellValue> {

    // Constants for Cassandra data types
    public static final String ASCII = "asciiT";
    public static final String BIGINT = "bigintT";
    public static final String BLOB = "blobT";
    public static final String BOOL = "boolT";
    public static final String COUNTER = "counterT";
    public static final String DATE = "dateT";
    public static final String DECIMAL = "decimalT";
    public static final String DOUBLE = "doubleT";
    public static final String FLOAT = "floatT";
    public static final String INET = "inetT";
    public static final String INT = "intT";
    public static final String LIST = "listT";
    public static final String MAP = "mapT";
    public static final String SET = "setT";
    public static final String SMALLINT = "smallintT";
    public static final String TEXT = "textT";
    public static final String TIME = "timeT";
    public static final String TIMEUUID = "timeuuidT";
    public static final String TINYINT = "tinyintT";
    public static final String TUPLE = "tupleT";
    public static final String UUID = "uuidT";
    public static final String VARCHAR = "varcharT";
    public static final String VARINT = "varintT";
    public static final String UDT = "udtT";

    @Override
    public void serialize(KeyspacesCellValue value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject();

        // Write scalar types
        if (value.asciiT() != null) gen.writeStringField(ASCII, value.asciiT());
        if (value.bigintT() != null) gen.writeStringField(BIGINT, value.bigintT());
        if (value.blobT() != null) gen.writeBinaryField(BLOB, value.blobT().asByteArray());
        if (value.boolT() != null) gen.writeBooleanField(BOOL, value.boolT());
        if (value.counterT() != null) gen.writeStringField(COUNTER, value.counterT());
        if (value.dateT() != null) gen.writeStringField(DATE, value.dateT());
        if (value.decimalT() != null) gen.writeStringField(DECIMAL, value.decimalT());
        if (value.doubleT() != null) gen.writeStringField(DOUBLE, value.doubleT());
        if (value.floatT() != null) gen.writeStringField(FLOAT, value.floatT());
        if (value.inetT() != null) gen.writeStringField(INET, value.inetT());
        if (value.intT() != null) gen.writeStringField(INT, value.intT());
        if (value.smallintT() != null) gen.writeStringField(SMALLINT, value.smallintT());
        if (value.textT() != null) gen.writeStringField(TEXT, value.textT());
        if (value.timeT() != null) gen.writeStringField(TIME, value.timeT());
        if (value.timeuuidT() != null) gen.writeStringField(TIMEUUID, value.timeuuidT());
        if (value.tinyintT() != null) gen.writeStringField(TINYINT, value.tinyintT());
        if (value.uuidT() != null) gen.writeStringField(UUID, value.uuidT());
        if (value.varcharT() != null) gen.writeStringField(VARCHAR, value.varcharT());
        if (value.varintT() != null) gen.writeStringField(VARINT, value.varintT());

        // Write collection types only if they're explicitly set
        if (value.hasListT()) {
            gen.writeFieldName(LIST);
            gen.writeObject(value.listT());
        }

        if (value.hasMapT()) {
            gen.writeFieldName(MAP);
            gen.writeObject(value.mapT());
        }

        if (value.hasSetT()) {
            gen.writeFieldName(SET);
            gen.writeObject(value.setT());
        }

        if (value.hasTupleT()) {
            gen.writeFieldName(TUPLE);
            gen.writeObject(value.tupleT());
        }

        if (value.hasUdtT()) {
            gen.writeFieldName(UDT);
            gen.writeObject(value.udtT());
        }

        gen.writeEndObject();
    }
}
