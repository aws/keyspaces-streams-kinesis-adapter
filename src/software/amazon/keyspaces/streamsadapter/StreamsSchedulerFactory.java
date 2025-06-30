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

import software.amazon.keyspaces.streamsadapter.processor.KeyspacesStreamsShardRecordProcessor;
import software.amazon.keyspaces.streamsadapter.util.KinesisMapperUtil;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.keyspacesstreams.KeyspacesStreamsClient;
import software.amazon.kinesis.checkpoint.CheckpointConfig;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.coordinator.CoordinatorConfig;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.lifecycle.LifecycleConfig;
import software.amazon.kinesis.metrics.MetricsConfig;
import software.amazon.kinesis.processor.FormerStreamsLeasesDeletionStrategy;
import software.amazon.kinesis.processor.ProcessorConfig;
import software.amazon.kinesis.processor.SingleStreamTracker;
import software.amazon.kinesis.processor.StreamTracker;
import software.amazon.kinesis.retrieval.DataFetcherProviderConfig;
import software.amazon.kinesis.retrieval.RetrievalConfig;
import software.amazon.kinesis.retrieval.polling.DataFetcher;
import software.amazon.kinesis.retrieval.polling.PollingConfig;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class StreamsSchedulerFactory {
    /**
     * Factory function for customers to create a stream tracker to consume multiple Keyspaces Streams from a single application.
     * @param keyspacesStreamArns
     * @param initialPositionInStreamExtended
     * @param formerStreamsLeasesDeletionStrategy
     * @return
     */
    public static StreamTracker createMultiStreamTracker(List<String> keyspacesStreamArns,
                                                         @NonNull InitialPositionInStreamExtended initialPositionInStreamExtended,
                                                         FormerStreamsLeasesDeletionStrategy formerStreamsLeasesDeletionStrategy) {
        if (keyspacesStreamArns == null || keyspacesStreamArns.isEmpty()) {
            throw new IllegalArgumentException("Stream ARN list cannot be empty");
        }
        for (String streamArn: keyspacesStreamArns) {
            if (!KinesisMapperUtil.isValidKeyspacesStreamArn(streamArn)) {
                throw new IllegalArgumentException("Invalid Keyspaces Stream ARN: " + streamArn);
            }
        }
        List<StreamConfig> streamConfigList = keyspacesStreamArns.stream()
                .map(streamArn -> new StreamConfig(
                        StreamIdentifier.multiStreamInstance(KinesisMapperUtil.createKinesisStreamIdentifierFromKeyspacesStreamsArn(streamArn, true)),
                        initialPositionInStreamExtended,
                        null
                ))
                .collect(Collectors.toList());
        return new KeyspacesStreamsMultiStreamTracker(streamConfigList, formerStreamsLeasesDeletionStrategy);
    }

    /**
     * Factory function for customers to create a stream tracker to consume a single Keyspaces Stream from a single application.
     * @param keyspacesStreamArn
     * @return
     */
    public static StreamTracker createSingleStreamTracker(
            String keyspacesStreamArn,
            InitialPositionInStreamExtended initialPositionInStreamExtended) {
        if (!KinesisMapperUtil.isValidKeyspacesStreamArn(keyspacesStreamArn)) {
            throw new IllegalArgumentException("Invalid Keyspaces Stream ARN: " + keyspacesStreamArn);
        }
        return new SingleStreamTracker(KinesisMapperUtil.createKinesisStreamIdentifierFromKeyspacesStreamsArn(keyspacesStreamArn, false), initialPositionInStreamExtended);
    }

    /**
     * Factory function for customers to create a scheduler.
     * Either createSingleStreamTracker or createMultiStreamTracker must be called before this function.
     * @param checkpointConfig
     * @param coordinatorConfig
     * @param leaseManagementConfig
     * @param lifecycleConfig
     * @param metricsConfig
     * @param processorConfig
     * @param retrievalConfig
     * @param credentialsProvider
     * @param region
     * @return
     */
    public static Scheduler createScheduler(
            @NonNull CheckpointConfig checkpointConfig,
            @NonNull CoordinatorConfig coordinatorConfig,
            @NonNull LeaseManagementConfig leaseManagementConfig,
            @NonNull LifecycleConfig lifecycleConfig,
            @NonNull MetricsConfig metricsConfig,
            @NonNull ProcessorConfig processorConfig,
            @NonNull RetrievalConfig retrievalConfig,
            @NonNull AwsCredentialsProvider credentialsProvider,
            @NonNull Region region
    ) {
        AmazonKeyspacesStreamsAdapterClient amazonKeyspacesStreamsAdapterClient = new AmazonKeyspacesStreamsAdapterClient(
                credentialsProvider,
                region
        );

        return createScheduler(checkpointConfig, coordinatorConfig,
                leaseManagementConfig, lifecycleConfig, metricsConfig,
                processorConfig, retrievalConfig, amazonKeyspacesStreamsAdapterClient);
    }

    /**
     * Factory function for customers to create a scheduler.
     * Either createSingleStreamTracker or createMultiStreamTracker must be called before this function.
     * @param checkpointConfig
     * @param coordinatorConfig
     * @param leaseManagementConfig
     * @param lifecycleConfig
     * @param metricsConfig
     * @param processorConfig
     * @param retrievalConfig
     * @param keyspacesStreamsClient
     * @param region
     * @return
     */
    public static Scheduler createScheduler(
            @NonNull CheckpointConfig checkpointConfig,
            @NonNull CoordinatorConfig coordinatorConfig,
            @NonNull LeaseManagementConfig leaseManagementConfig,
            @NonNull LifecycleConfig lifecycleConfig,
            @NonNull MetricsConfig metricsConfig,
            @NonNull ProcessorConfig processorConfig,
            @NonNull RetrievalConfig retrievalConfig,
            @NonNull KeyspacesStreamsClient keyspacesStreamsClient,
            @NonNull Region region
    ) {
        AmazonKeyspacesStreamsAdapterClient amazonKeyspacesStreamsAdapterClient = new AmazonKeyspacesStreamsAdapterClient(
                keyspacesStreamsClient,
                region
        );
        return createScheduler(checkpointConfig, coordinatorConfig,
                leaseManagementConfig, lifecycleConfig, metricsConfig,
                processorConfig, retrievalConfig, amazonKeyspacesStreamsAdapterClient);
    }

    private static Scheduler createScheduler(
            @NonNull CheckpointConfig checkpointConfig,
            @NonNull CoordinatorConfig coordinatorConfig,
            @NonNull LeaseManagementConfig leaseManagementConfig,
            @NonNull LifecycleConfig lifecycleConfig,
            @NonNull MetricsConfig metricsConfig,
            @NonNull ProcessorConfig processorConfig,
            @NonNull RetrievalConfig retrievalConfig,
            @NonNull AmazonKeyspacesStreamsAdapterClient amazonKeyspacesStreamsAdapterClient
    ) {
        if (!(processorConfig.shardRecordProcessorFactory().shardRecordProcessor() instanceof KeyspacesStreamsShardRecordProcessor)) {
            throw new IllegalArgumentException("ShardRecordProcessor should be of type KeyspacesStreamsShardRecordProcessor");
        }

        if (!(retrievalConfig.retrievalSpecificConfig() instanceof PollingConfig)) {
            throw new IllegalArgumentException("RetrievalConfig should be of type PollingConfig");
        }

        Function<DataFetcherProviderConfig, DataFetcher> dataFetcherProvider =
                (dataFetcherProviderConfig) -> new KeyspacesStreamsDataFetcher(
                        amazonKeyspacesStreamsAdapterClient,
                        dataFetcherProviderConfig);
        PollingConfig pollingConfig = (PollingConfig) retrievalConfig.retrievalSpecificConfig();
        pollingConfig.dataFetcherProvider(dataFetcherProvider);

        pollingConfig.sleepTimeController(new KeyspacesStreamsSleepTimeController());

        retrievalConfig.retrievalSpecificConfig(pollingConfig);

        if (!coordinatorConfig.skipShardSyncAtWorkerInitializationIfLeasesExist()) {
            log.warn("skipShardSyncAtWorkerInitializationIfLeasesExist is not set to true. " +
                    "This will cause the worker to delay working on lease. Setting this to true");
            coordinatorConfig.skipShardSyncAtWorkerInitializationIfLeasesExist(true);
        }

        KeyspacesStreamsLeaseManagementFactory keyspacesStreamsLeaseManagementFactory =
                new KeyspacesStreamsLeaseManagementFactory(
                        amazonKeyspacesStreamsAdapterClient,
                        leaseManagementConfig,
                        retrievalConfig
                );
        leaseManagementConfig.leaseManagementFactory(keyspacesStreamsLeaseManagementFactory);
        leaseManagementConfig.consumerTaskFactory(new KeyspacesStreamsConsumerTaskFactory());

        return new Scheduler(
                checkpointConfig,
                coordinatorConfig,
                leaseManagementConfig,
                lifecycleConfig,
                metricsConfig,
                processorConfig,
                retrievalConfig
        );
    }
}
