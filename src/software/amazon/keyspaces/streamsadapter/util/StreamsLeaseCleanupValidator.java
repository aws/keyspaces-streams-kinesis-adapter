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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.MultiStreamLease;

import java.util.Set;

public class StreamsLeaseCleanupValidator {
    private static final Log LOG = LogFactory.getLog(StreamsLeaseCleanupValidator.class);

    /**
     * Validates if a lease is candidate for cleanup in multi-stream mode.
     *
     * @param lease Candidate shard we are considering for deletion.
     * @param currentKinesisShardIds List of leases currently held by the worker.
     * @param isMultiStreamMode Whether running in multi-stream mode
     * @return true if neither the shard (corresponding to the lease), nor its parents are present in
     *         currentKinesisShardIds
     */
    public static boolean isCandidateForCleanup(Lease lease,
                                                Set<String> currentKinesisShardIds,
                                                boolean isMultiStreamMode) {
        boolean isCandidateForCleanup = true;

        // Extract the correct shardId based on stream mode
        String shardId = isMultiStreamMode ?
                ((MultiStreamLease) lease).shardId() :
                lease.leaseKey();

        if (currentKinesisShardIds.contains(shardId)) {
            isCandidateForCleanup = false;
        } else {
            LOG.info("Found lease for non-existent shard: " + shardId + ". Checking its parent shards");
            Set<String> parentShardIds = lease.parentShardIds();
            for (String parentShardId : parentShardIds) {
                // Return false if parent shard exists (but the child does not).
                // This may be a (rare) race condition between fetching the shard list and Kinesis expiring shards.
                if (currentKinesisShardIds.contains(parentShardId)) {
                    String message = "Parent shard " + parentShardId + " exists but not the child shard " + shardId;
                    LOG.error(message);
                    return false;
                }
            }
        }

        return isCandidateForCleanup;
    }

    /**
     * Overloaded method that defaults to single-stream mode for backward compatibility
     */
    public static boolean isCandidateForCleanup(Lease lease, Set<String> currentKinesisShardIds) {
        return isCandidateForCleanup(lease, currentKinesisShardIds, false);
    }
}