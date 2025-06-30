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

import software.amazon.keyspaces.streamsadapter.AmazonKeyspacesStreamsAdapterClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.keyspacesstreams.model.AccessDeniedException;
import software.amazon.awssdk.services.keyspacesstreams.model.InternalServerException;
import software.amazon.awssdk.services.keyspacesstreams.model.ResourceNotFoundException;
import software.amazon.awssdk.services.keyspacesstreams.model.ThrottlingException;
import software.amazon.awssdk.services.keyspacesstreams.model.ValidationException;
import software.amazon.awssdk.services.keyspacesstreams.model.ValidationExceptionType;
import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException;
import software.amazon.awssdk.services.kinesis.model.ExpiredNextTokenException;
import software.amazon.awssdk.services.kinesis.model.InvalidArgumentException;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;
import software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException;

import software.amazon.keyspaces.streamsadapter.exceptions.UnableToReadMoreRecordsException;

public class AmazonServiceExceptionTransformer {

    private static final String TRIMMED_DATA_KCL_RETRY_MESSAGE =
            "Attempted to get a shard iterator for a trimmed shard. Data has been lost";

    public static final String KEYSPACES_STREAMS_THROTTLING_EXCEPTION_ERROR_CODE = "ThrottlingException";

    public static final String EMPTY_STRING = "";
    private static final Log LOG = LogFactory.getLog(AmazonServiceExceptionTransformer.class);

    private enum ThrottlingHandler {
        LIMIT_EXCEEDED {
            @Override
            AwsServiceException.Builder handle(AwsServiceException ase) {
                return LimitExceededException.builder().message(buildErrorMessage(ase));
            }
        },
        PROVISIONED_THROUGHPUT_EXCEEDED {
            @Override
            AwsServiceException.Builder handle(AwsServiceException ase) {
                return ProvisionedThroughputExceededException.builder().message(buildErrorMessage(ase));
            }
        };

        abstract AwsServiceException.Builder handle(AwsServiceException ase);
    }

    // Common transformation rules
    private static class TransformationRules {
        static AwsServiceException.Builder handleInternalServer(AwsServiceException ase) {
            return AwsServiceException.builder()
                    .message(buildErrorMessage(ase))
                    .cause(ase);
        }

        static AwsServiceException.Builder handleResourceNotFound(AwsServiceException ase) {
            return software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException.builder()
                    .message(buildErrorMessage(ase));
        }

        static AwsServiceException.Builder handleAccessDenied(AwsServiceException ase) {
            return software.amazon.awssdk.services.kinesis.model.AccessDeniedException.builder()
                    .message(buildErrorMessage(ase));
        }
    }

    // Common transformation logic
    private static AwsServiceException transformException(
            AwsServiceException ase,
            AmazonKeyspacesStreamsAdapterClient.SkipRecordsBehavior skipRecordsBehavior,
            boolean isGetRecords,
            ThrottlingHandler throttlingHandler) {

        if (ase == null) return null;

        final AwsServiceException.Builder transforming;

        if (ase instanceof InternalServerException) {
            transforming = TransformationRules.handleInternalServer(ase);
        } else if (ase instanceof ResourceNotFoundException) {
            transforming = TransformationRules.handleResourceNotFound(ase);
        } else if (ase instanceof ThrottlingException) {
            transforming = throttlingHandler.handle(ase);
        } else if (ase instanceof AccessDeniedException) {
            transforming = TransformationRules.handleAccessDenied(ase);
        } else if (ase instanceof ValidationException) {
            transforming = transformValidationExceptionToKinesisException(
                    (ValidationException) ase, skipRecordsBehavior, isGetRecords);
        } else {
            transforming = null;
        }

        return applyFields(ase, transforming);
    }

    private static AwsServiceException applyFields(AwsServiceException original,
                                                   AwsServiceException.Builder transforming) {
        if (transforming == null) {
            LOG.error("Could not transform a Keyspaces AwsServiceException to a compatible Kinesis exception",
                    original);
            return original;
        }

        AwsErrorDetails.Builder transformingDetailsBuilder = getDetailsBuilder(transforming.awsErrorDetails());

        if (original.awsErrorDetails() != null) {
            if (original.awsErrorDetails().errorCode() != null) {
                transformingDetailsBuilder.errorCode(original.awsErrorDetails().errorCode());
            }
            if (original.awsErrorDetails().serviceName() != null) {
                transformingDetailsBuilder.serviceName(original.awsErrorDetails().serviceName());
            }
        }

        transforming.awsErrorDetails(transformingDetailsBuilder.build());

        if (original.requestId() != null) {
            transforming.requestId(original.requestId());
        }
        transforming.statusCode(original.statusCode());

        LOG.debug(String.format("Keyspaces Streams exception: %s transformed to Kinesis %s",
                original.getClass(), transforming.getClass()), original);
        return transforming.build();
    }

    private static AwsErrorDetails.Builder getDetailsBuilder(AwsErrorDetails aed) {
        return aed == null ? AwsErrorDetails.builder() : aed.toBuilder();
    }

    private static String buildErrorMessage(AwsServiceException ase) {
        return ase.getMessage() == null ? EMPTY_STRING : ase.getMessage();
    }

    public static AwsServiceException transformKeyspacesStreamsToKinesisListStreams(
            AwsServiceException ase, AmazonKeyspacesStreamsAdapterClient.SkipRecordsBehavior skipRecordsBehavior) {
        return transformException(ase, skipRecordsBehavior, false, ThrottlingHandler.LIMIT_EXCEEDED);
    }

    public static AwsServiceException transformKeyspacesStreamsToKinesisDescribeStream(
            AwsServiceException ase, AmazonKeyspacesStreamsAdapterClient.SkipRecordsBehavior skipRecordsBehavior) {
        return transformException(ase, skipRecordsBehavior, false, ThrottlingHandler.LIMIT_EXCEEDED);
    }

    public static AwsServiceException transformKeyspacesStreamsToKinesisGetShardIterator(
            AwsServiceException ase, AmazonKeyspacesStreamsAdapterClient.SkipRecordsBehavior skipRecordsBehavior) {
        if (skipRecordsBehavior == AmazonKeyspacesStreamsAdapterClient.SkipRecordsBehavior.KCL_RETRY) {
            throw new UnableToReadMoreRecordsException(TRIMMED_DATA_KCL_RETRY_MESSAGE, ase);
        }
        return transformException(ase, skipRecordsBehavior, false,
                ThrottlingHandler.PROVISIONED_THROUGHPUT_EXCEEDED);
    }

    public static AwsServiceException transformKeyspacesStreamsToKinesisGetRecords(
            AwsServiceException ase, AmazonKeyspacesStreamsAdapterClient.SkipRecordsBehavior skipRecordsBehavior) {
        return transformException(ase, skipRecordsBehavior, true,
                ThrottlingHandler.PROVISIONED_THROUGHPUT_EXCEEDED);
    }

    /**
     * Maps Keyspaces Streams ValidationException to Kinesis exceptions. INVALID_FORMAT maps to InvalidArgumentException,
     * TRIMMED_DATA_ACCESS handling varies (see handleTrimmedDataAccessException), EXPIRED_ITERATOR maps to
     * ExpiredIteratorException, and EXPIRED_NEXT_TOKEN maps to ExpiredNextTokenException.
     *
     * @param validationException The ValidationException to transform
     * @param skipRecordsBehavior The behavior to use for trimmed data
     * @param isGetRecords Whether this is for a GetRecords operation
     * @return A compatible Amazon Kinesis exception
     */
    private static AwsServiceException.Builder transformValidationExceptionToKinesisException(
            ValidationException validationException,
            AmazonKeyspacesStreamsAdapterClient.SkipRecordsBehavior skipRecordsBehavior,
            boolean isGetRecords) {

        final AwsServiceException.Builder transforming;

        if (validationException.errorCode() == ValidationExceptionType.INVALID_FORMAT) {
            transforming = InvalidArgumentException.builder()
                    .message(buildErrorMessage(validationException));
        } else if (validationException.errorCode() == ValidationExceptionType.TRIMMED_DATA_ACCESS) {
            transforming = handleTrimmedDataAccessException(
                    validationException, skipRecordsBehavior, isGetRecords);
        } else if (validationException.errorCode() == ValidationExceptionType.EXPIRED_ITERATOR) {
            transforming = ExpiredIteratorException.builder()
                    .message(buildErrorMessage(validationException));
        } else if (validationException.errorCode() == ValidationExceptionType.EXPIRED_NEXT_TOKEN) {
            transforming = ExpiredNextTokenException.builder()
                    .message(buildErrorMessage(validationException));
        } else {
            transforming = null;
        }
        return transforming;
    }

    /**
     * Handles TRIMMED_DATA_ACCESS validation exceptions. For GetRecords operations with SKIP_RECORDS_TO_TRIM_HORIZON,
     * returns ExpiredIteratorException.
     * For other operations with SKIP_RECORDS_TO_TRIM_HORIZON, returns ResourceNotFoundException.
     * For all operations with KCL_RETRY, throws UnableToReadMoreRecordsException.
     *
     * @param validationException The ValidationException with TRIMMED_DATA_ACCESS error code
     * @param skipRecordsBehavior The configured behavior for handling trimmed data
     * @param isGetRecords Whether this exception occurred during a GetRecords operation
     * @return A compatible Amazon Kinesis exception based on the parameters
     * @throws UnableToReadMoreRecordsException if skipRecordsBehavior is KCL_RETRY
     */
    private static AwsServiceException.Builder handleTrimmedDataAccessException(
            ValidationException validationException,
            AmazonKeyspacesStreamsAdapterClient.SkipRecordsBehavior skipRecordsBehavior,
            boolean isGetRecords) {

        final AwsServiceException.Builder transforming;

        if (isGetRecords) {
            if (skipRecordsBehavior == AmazonKeyspacesStreamsAdapterClient.SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON) {
                transforming = ExpiredIteratorException.builder()
                        .message(buildErrorMessage(validationException));
            } else {
                throw new UnableToReadMoreRecordsException(
                        TRIMMED_DATA_KCL_RETRY_MESSAGE,
                        validationException);
            }
        } else {
            if (skipRecordsBehavior == AmazonKeyspacesStreamsAdapterClient.SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON) {
                transforming = software.amazon.awssdk.services.kinesis.model
                        .ResourceNotFoundException.builder()
                        .message(buildErrorMessage(validationException));
            } else {
                throw new UnableToReadMoreRecordsException(
                        TRIMMED_DATA_KCL_RETRY_MESSAGE,
                        validationException);
            }
        }
        return transforming;
    }
}
