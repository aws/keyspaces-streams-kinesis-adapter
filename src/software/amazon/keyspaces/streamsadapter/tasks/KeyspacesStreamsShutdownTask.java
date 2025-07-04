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

package software.amazon.keyspaces.streamsadapter.tasks;

import com.google.common.annotations.VisibleForTesting;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.kinesis.model.ChildShard;
import software.amazon.awssdk.utils.CollectionUtils;
import software.amazon.kinesis.checkpoint.ShardRecordProcessorCheckpointer;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.leases.HierarchicalShardSyncer;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseCleanupManager;
import software.amazon.kinesis.leases.LeaseCoordinator;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.ShardDetector;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.leases.UpdateField;
import software.amazon.kinesis.leases.exceptions.CustomerApplicationException;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.lifecycle.ConsumerTask;
import software.amazon.kinesis.lifecycle.ShutdownReason;
import software.amazon.kinesis.lifecycle.TaskResult;
import software.amazon.kinesis.lifecycle.TaskType;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.metrics.MetricsScope;
import software.amazon.kinesis.metrics.MetricsUtil;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.RecordsPublisher;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Task for invoking the ShardRecordProcessor shutdown() callback.
 */
@RequiredArgsConstructor
@Slf4j
public class KeyspacesStreamsShutdownTask implements ConsumerTask {
    private static final String SHUTDOWN_TASK_OPERATION = "KeyspacesStreamsShutdownTask";
    private static final String RECORD_PROCESSOR_SHUTDOWN_METRIC = "RecordProcessor.shutdown";

    /**
     * Reusable, immutable {@link LeaseLostInput}.
     */
    private static final LeaseLostInput LEASE_LOST_INPUT =
            LeaseLostInput.builder().build();

    private static final Random RANDOM = new Random();

    @VisibleForTesting
    static final int RETRY_RANDOM_MAX_RANGE = 30;

    @NonNull
    private final ShardInfo shardInfo;

    @NonNull
    private final ShardDetector keyspacesStreamsShardDetector;

    @NonNull
    private final ShardRecordProcessor shardRecordProcessor;

    @NonNull
    private final ShardRecordProcessorCheckpointer recordProcessorCheckpointer;

    @NonNull
    private final ShutdownReason reason;

    @NonNull
    private final InitialPositionInStreamExtended initialPositionInStream;

    private final boolean cleanupLeasesOfCompletedShards;
    private final boolean ignoreUnexpectedChildShards;

    @NonNull
    private final LeaseCoordinator leaseCoordinator;

    private final long backoffTimeMillis;

    @NonNull
    private final RecordsPublisher recordsPublisher;

    @NonNull
    private final HierarchicalShardSyncer keyspacesStreamsShardSyncer;

    @NonNull
    private final MetricsFactory metricsFactory;

    private final TaskType taskType = TaskType.SHUTDOWN;

    private final List<ChildShard> childShards;

    @NonNull
    private final StreamIdentifier streamIdentifier;

    @NonNull
    private final LeaseCleanupManager leaseCleanupManager;

    @Override
    public TaskResult call() {
        recordProcessorCheckpointer.checkpointer().operation(SHUTDOWN_TASK_OPERATION);
        final MetricsScope scope = MetricsUtil.createMetricsWithOperation(metricsFactory, SHUTDOWN_TASK_OPERATION);

        final String leaseKey = ShardInfo.getLeaseKey(shardInfo);

        try {
            log.debug(
                    "Invoking shutdown() for shard {} with childShards {}, concurrencyToken {}. Shutdown reason: {}",
                    leaseKey,
                    childShards,
                    shardInfo.concurrencyToken(),
                    reason);

            final long startTime = System.currentTimeMillis();
            final Lease currentShardLease = leaseCoordinator.getCurrentlyHeldLease(leaseKey);
            final Runnable leaseLostAction = () -> shardRecordProcessor.leaseLost(LEASE_LOST_INPUT);

            if (reason == ShutdownReason.SHARD_END) {
                try {
                    takeShardEndAction(currentShardLease, leaseKey, scope, startTime);
                } catch (InvalidStateException e) {
                    // If InvalidStateException happens, it indicates we have a non recoverable error in short term.
                    // In this scenario, we should shutdown the shardConsumer with LEASE_LOST reason to allow
                    // other worker to take the lease and retry shutting down.
                    log.warn(
                            "Lease {}: Invalid state encountered while shutting down shardConsumer with SHARD_END reason. "
                                    + "Dropping the lease and shutting down shardConsumer using LEASE_LOST reason.",
                            leaseKey,
                            e);
                    dropLease(currentShardLease, leaseKey);
                    throwOnApplicationException(leaseKey, leaseLostAction, scope, startTime);
                }
            } else {
                throwOnApplicationException(leaseKey, leaseLostAction, scope, startTime);
            }
            log.debug("Shutting down retrieval strategy for shard {}.", leaseKey);
            recordsPublisher.shutdown();

            log.debug("Record processor completed shutdown() for shard {}", leaseKey);

            return new TaskResult(null);
        } catch (Exception e) {
            if (e instanceof CustomerApplicationException) {
                log.error("Shard {}: Application exception.", leaseKey, e);
            } else {
                log.error("Shard {}: Caught exception:", leaseKey, e);
            }
            // backoff if we encounter an exception.
            try {
                Thread.sleep(this.backoffTimeMillis);
            } catch (InterruptedException ie) {
                log.debug("Shard {}: Interrupted sleep", leaseKey, ie);
            }

            return new TaskResult(e);
        } finally {
            MetricsUtil.endScope(scope);
        }
    }

    private void takeShardEndAction(Lease currentShardLease, final String leaseKey, MetricsScope scope, long startTime)
            throws InvalidStateException, ProvisionedThroughputException, DependencyException, CustomerApplicationException {
        if (currentShardLease == null) {
            throw new InvalidStateException(
                    leaseKey + " : Lease not owned by the current worker. Leaving ShardEnd handling to new owner.");
        }
        if (!CollectionUtils.isNullOrEmpty(childShards)) {
            createLeasesForChildShardsIfNotExist(scope);
            updateLeaseWithChildShards(currentShardLease);
        }
        attemptShardEndCheckpointing(leaseKey, scope, startTime);
    }

    private boolean attemptShardEndCheckpointing(final String leaseKey, MetricsScope scope, long startTime)
            throws DependencyException, ProvisionedThroughputException, InvalidStateException,
            CustomerApplicationException {
        final Lease leaseFromDdb = Optional.ofNullable(
                        leaseCoordinator.leaseRefresher().getLease(leaseKey))
                .orElseThrow(() -> new InvalidStateException("Lease for shard " + leaseKey + " does not exist."));
        if (!leaseFromDdb.checkpoint().equals(ExtendedSequenceNumber.SHARD_END)) {
            // Call the shardRecordsProcessor to checkpoint with SHARD_END sequence number.
            // The shardEnded is implemented by customer. We should validate if the SHARD_END checkpointing is
            // successful after calling shardEnded.
            throwOnApplicationException(
                    leaseKey, () -> applicationCheckpointAndVerification(leaseKey), scope, startTime);
        }
        return true;
    }

    private void applicationCheckpointAndVerification(final String leaseKey) {
        recordProcessorCheckpointer.sequenceNumberAtShardEnd(
                recordProcessorCheckpointer.largestPermittedCheckpointValue());
        recordProcessorCheckpointer.largestPermittedCheckpointValue(ExtendedSequenceNumber.SHARD_END);
        shardRecordProcessor.shardEnded(ShardEndedInput.builder()
                .checkpointer(recordProcessorCheckpointer)
                .build());
        final ExtendedSequenceNumber lastCheckpointValue = recordProcessorCheckpointer.lastCheckpointValue();
        if (!ExtendedSequenceNumber.SHARD_END.equals(lastCheckpointValue)) {
            throw new IllegalArgumentException("Application didn't checkpoint at end of shard "
                    + leaseKey + ". Application must checkpoint upon shard end. "
                    + "See ShardRecordProcessor.shardEnded javadocs for more information.");
        }
    }

    private void throwOnApplicationException(
            final String leaseKey, Runnable action, MetricsScope metricsScope, final long startTime)
            throws CustomerApplicationException {
        try {
            action.run();
        } catch (Exception e) {
            throw new CustomerApplicationException(
                    "Customer application throws exception for shard " + leaseKey + ": ", e);
        } finally {
            MetricsUtil.addLatency(metricsScope, RECORD_PROCESSOR_SHUTDOWN_METRIC, startTime, MetricsLevel.SUMMARY);
        }
    }

    private void createLeasesForChildShardsIfNotExist(MetricsScope scope)
            throws ProvisionedThroughputException, InvalidStateException, DependencyException {
        final LeaseRefresher leaseRefresher = leaseCoordinator.leaseRefresher();
        for (ChildShard childShard: childShards) {
            final String leaseKey = ShardInfo.getLeaseKey(shardInfo, childShard.shardId());
            if (leaseRefresher.getLease(leaseKey) == null) {
                log.debug(
                        "{} - Shard {} - Attempting to create lease for child shard {}",
                        keyspacesStreamsShardDetector.streamIdentifier(),
                        shardInfo.shardId(),
                        leaseKey);
                final Lease leaseToCreate = keyspacesStreamsShardSyncer.createLeaseForChildShard(childShard, streamIdentifier);
                final long startTime = System.currentTimeMillis();
                boolean success = false;
                try {
                    leaseRefresher.createLeaseIfNotExists(leaseToCreate);
                    success = true;
                } finally {
                    MetricsUtil.addSuccessAndLatency(scope, "CreateLease", success, startTime, MetricsLevel.DETAILED);
                    if (leaseToCreate.checkpoint() != null) {
                        final String metricName = leaseToCreate.checkpoint().isSentinelCheckpoint()
                                ? leaseToCreate.checkpoint().sequenceNumber()
                                : "SEQUENCE_NUMBER";
                        MetricsUtil.addSuccess(scope, "CreateLease_" + metricName, true, MetricsLevel.DETAILED);
                    }
                }

                log.info(
                        "{} - Shard {}: Created child shard lease: {}",
                        keyspacesStreamsShardDetector.streamIdentifier(),
                        shardInfo.shardId(),
                        leaseToCreate);
            }
        }
    }

    private void updateLeaseWithChildShards(Lease currentLease)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        Set<String> childShardIds =
                childShards.stream().map(ChildShard::shardId).collect(Collectors.toSet());

        final Lease updatedLease = currentLease.copy();
        updatedLease.childShardIds(childShardIds);
        leaseCoordinator.leaseRefresher().updateLeaseWithMetaInfo(updatedLease, UpdateField.CHILD_SHARDS);
        log.info(
                "Shard {}: Updated current lease {} with child shard information: {}",
                shardInfo.shardId(),
                currentLease.leaseKey(),
                childShardIds);
    }

    /*
     * (non-Javadoc)
     *
     * @see com.amazonaws.services.kinesis.clientlibrary.lib.worker.ConsumerTask#taskType()
     */
    @Override
    public TaskType taskType() {
        return taskType;
    }

    @VisibleForTesting
    public ShutdownReason getReason() {
        return reason;
    }

    private void dropLease(Lease currentLease, final String leaseKey) {
        if (currentLease == null) {
            log.warn(
                    "Shard {}: Unable to find the lease for shard. Will shutdown the shardConsumer directly.",
                    leaseKey);
        } else {
            leaseCoordinator.dropLease(currentLease);
            log.info("Dropped lease for shutting down ShardConsumer: " + currentLease.leaseKey());
        }
    }
}
