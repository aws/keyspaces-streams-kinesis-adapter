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

package software.amazon.keyspaces.streamsadapter.adapter;

import software.amazon.awssdk.services.keyspacesstreams.model.GetRecordsResponse;
import software.amazon.awssdk.services.keyspacesstreams.model.Record;
import software.amazon.awssdk.services.kinesis.model.ChildShard;
import software.amazon.kinesis.retrieval.GetRecordsResponseAdapter;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class KeyspacesStreamsGetRecordsResponseAdapter implements GetRecordsResponseAdapter {

    private final GetRecordsResponse response;

    public KeyspacesStreamsGetRecordsResponseAdapter(GetRecordsResponse response) {
        this.response = response;
    }

    @Override
    public List<KinesisClientRecord> records() {
        return response.changeRecords().stream()
                .map(KeyspacesStreamsClientRecord::fromRecord)
                .collect(Collectors.toList());
    }

    @Override
    public Long millisBehindLatest() {
        if (response.changeRecords().isEmpty()) {
            return null;
        } else {
            Record lastRecord = response.changeRecords().get(response.changeRecords().size() - 1);
            long recordTimestamp = lastRecord.createdAt().toEpochMilli();
            long currentTime = System.currentTimeMillis();
            return Math.max(0, currentTime - recordTimestamp);
        }
    }

    @Override
    public List<ChildShard> childShards() {
        return Collections.emptyList();
    }

    @Override
    public String nextShardIterator() {
        return response.nextShardIterator();
    }

    @Override
    public String requestId() {
        return response.responseMetadata().requestId();
    }
}

