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

package software.amazon.keyspaces.streamsadapter.processor;

import software.amazon.keyspaces.streamsadapter.adapter.KeyspacesStreamsClientRecord;
import software.amazon.keyspaces.streamsadapter.model.KeyspacesStreamsProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.processor.ShardRecordProcessor;

import java.util.stream.Collectors;

public interface KeyspacesStreamsShardRecordProcessor extends ShardRecordProcessor {
    @Override
    default void processRecords(ProcessRecordsInput processRecordsInput) {
        KeyspacesStreamsProcessRecordsInput keyspacesStreamsProcessRecordsInput = KeyspacesStreamsProcessRecordsInput.builder()
                .records(processRecordsInput.records().stream()
                        .map(kinesisClientRecord -> (KeyspacesStreamsClientRecord) kinesisClientRecord)
                        .collect(Collectors.toList()))
                .checkpointer(processRecordsInput.checkpointer())
                .cacheEntryTime(processRecordsInput.cacheEntryTime())
                .cacheExitTime(processRecordsInput.cacheExitTime())
                .millisBehindLatest(processRecordsInput.millisBehindLatest())
                .childShards(processRecordsInput.childShards())
                .isAtShardEnd(processRecordsInput.isAtShardEnd())
                .build();
        processRecords(keyspacesStreamsProcessRecordsInput);
    }

    void processRecords(KeyspacesStreamsProcessRecordsInput processRecordsInput);
}
