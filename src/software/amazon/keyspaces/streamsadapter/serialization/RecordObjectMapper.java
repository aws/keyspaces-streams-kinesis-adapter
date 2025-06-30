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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Map;
import software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesCell;
import software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesCellMapDefinition;
import software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesCellValue;
import software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesMetadata;
import software.amazon.awssdk.services.keyspacesstreams.model.KeyspacesRow;
import software.amazon.awssdk.services.keyspacesstreams.model.Record;

public class RecordObjectMapper extends ObjectMapper {
    public static final String EVENT_VERSION = "eventVersion";
    public static final String CREATED_AT = "createdAt";
    public static final String ORIGIN = "origin";
    public static final String PARTITION_KEYS = "partitionKeys";
    public static final String CLUSTERING_KEYS = "clusteringKeys";
    public static final String OLD_IMAGE = "oldImage";
    public static final String NEW_IMAGE = "newImage";
    public static final String SEQUENCE_NUMBER = "sequenceNumber";

    public static final String VALUE_CELLS = "valueCells";
    public static final String STATIC_CELLS = "staticCells";
    public static final String ROW_METADATA = "rowMetadata";
    public static final String METADATA = "metadata";

    public static final String KEY = "key";
    public static final String VALUE = "value";

    public static final String EXPIRATION_TIME = "expirationTime";
    public static final String WRITE_TIME = "writeTime";

    private static final String MODULE = "custom";

    public RecordObjectMapper() {
        super();
        // Disable failing on empty beans
        this.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        this.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        SimpleModule module = new SimpleModule(MODULE, Version.unknownVersion());

        // Custom serializer for KeyspacesCellValue to handle empty collections correctly
        module.addSerializer(KeyspacesCellValue.class, new KeyspacesCellValueSerializer());

        // Deal with (de)serializing of byte[].
        module.addSerializer(ByteBuffer.class, new ByteBufferSerializer());
        module.addDeserializer(ByteBuffer.class, new ByteBufferDeserializer());

        // Register the module
        this.registerModule(module);

        // Register JavaTimeModule to handle Java 8 date/time types
        this.registerModule(new JavaTimeModule());

        // Configure Instant serialization to use numeric timestamp
        this.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, true);

        this.addMixIn(Record.class, RecordMixIn.class);
        this.addMixIn(KeyspacesRow.class, KeyspacesRowMixIn.class);
        this.addMixIn(KeyspacesCell.class, KeyspacesCellMixIn.class);
        this.addMixIn(KeyspacesCellMapDefinition.class, KeyspacesCellMapDefinitionMixIn.class);
        this.addMixIn(KeyspacesMetadata.class, KeyspacesMetadataMixIn.class);
    }

    /*
     * Serializer and Deserializer classes
     */
    private static class ByteBufferSerializer extends JsonSerializer<ByteBuffer> {
        @Override
        public void serialize(ByteBuffer value, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonProcessingException {
            // value is never null, according to JsonSerializer contract
            jgen.writeBinary(value.array());
        }
    }

    private static class ByteBufferDeserializer extends JsonDeserializer<ByteBuffer> {
        @Override
        public ByteBuffer deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
            // never called for null literal, according to JsonDeserializer contract
            return ByteBuffer.wrap(jp.getBinaryValue());
        }
    }

    private static abstract class RecordMixIn {
        @JsonProperty(EVENT_VERSION)
        public abstract String eventVersion();

        @JsonProperty(CREATED_AT)
        public abstract Instant createdAt();

        @JsonProperty(ORIGIN)
        public abstract String origin();

        @JsonProperty(PARTITION_KEYS)
        public abstract Map<String, KeyspacesCellValue> partitionKeys();

        @JsonProperty(CLUSTERING_KEYS)
        public abstract Map<String, KeyspacesCellValue> clusteringKeys();

        @JsonProperty(NEW_IMAGE)
        public abstract KeyspacesRow newImage();

        @JsonProperty(OLD_IMAGE)
        public abstract KeyspacesRow oldImage();

        @JsonProperty(SEQUENCE_NUMBER)
        public abstract String sequenceNumber();
    }

    private static abstract class KeyspacesRowMixIn {
        @JsonProperty(VALUE_CELLS)
        public abstract Map<String, KeyspacesCell> valueCells();

        @JsonProperty(STATIC_CELLS)
        public abstract Map<String, KeyspacesCell> staticCells();

        @JsonProperty(ROW_METADATA)
        public abstract KeyspacesMetadata rowMetadata();
    }

    private static abstract class KeyspacesCellMapDefinitionMixIn {
        @JsonProperty(KEY)
        public abstract KeyspacesCellValue key();

        @JsonProperty(VALUE)
        public abstract KeyspacesCellValue value();

        @JsonProperty(METADATA)
        public abstract KeyspacesMetadata metadata();
    }

    private static abstract class KeyspacesMetadataMixIn {
        @JsonProperty(EXPIRATION_TIME)
        public abstract String expirationTime();

        @JsonProperty(WRITE_TIME)
        public abstract String writeTime();
    }

    private static abstract class KeyspacesCellMixIn {
        @JsonProperty(VALUE)
        public abstract KeyspacesCellValue value();

        @JsonProperty(METADATA)
        public abstract KeyspacesMetadata metadata();
    }
}

