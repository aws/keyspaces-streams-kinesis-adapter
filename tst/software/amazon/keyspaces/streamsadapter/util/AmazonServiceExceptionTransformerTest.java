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
import software.amazon.keyspaces.streamsadapter.exceptions.UnableToReadMoreRecordsException;
import org.junit.jupiter.api.Test;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AmazonServiceExceptionTransformerTest {

    private static final String TEST_ERROR_MESSAGE = "Test error message";
    private static final String TEST_REQUEST_ID = "test-request-id";

    @Test
    void testTransformDescribeStreamInternalServerError() {
        InternalServerException original = createException(
                InternalServerException.builder(), TEST_ERROR_MESSAGE).build();

        AwsServiceException transformed = AmazonServiceExceptionTransformer
                .transformKeyspacesStreamsToKinesisDescribeStream(original,
                        AmazonKeyspacesStreamsAdapterClient.SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON);

        assertNotNull(transformed);
        assertEquals(original.statusCode(), transformed.statusCode());
    }

    @Test
    void testTransformDescribeStreamResourceNotFound() {
        ResourceNotFoundException original = createException(
                ResourceNotFoundException.builder(), TEST_ERROR_MESSAGE).build();

        AwsServiceException transformed = AmazonServiceExceptionTransformer
                .transformKeyspacesStreamsToKinesisDescribeStream(original,
                        AmazonKeyspacesStreamsAdapterClient.SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON);

        assertNotNull(transformed);
        assertInstanceOf(software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException.class, transformed);
    }

    @Test
    void testTransformDescribeStreamThrottling() {
        ThrottlingException original = createException(
                ThrottlingException.builder(), TEST_ERROR_MESSAGE).build();

        AwsServiceException transformed = AmazonServiceExceptionTransformer
                .transformKeyspacesStreamsToKinesisDescribeStream(original,
                        AmazonKeyspacesStreamsAdapterClient.SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON);

        assertNotNull(transformed);
        assertInstanceOf(LimitExceededException.class, transformed);
    }

    @Test
    void testTransformDescribeStreamAccessDenied() {
        AccessDeniedException original = createException(
                AccessDeniedException.builder(), TEST_ERROR_MESSAGE).build();

        AwsServiceException transformed = AmazonServiceExceptionTransformer
                .transformKeyspacesStreamsToKinesisDescribeStream(original,
                        AmazonKeyspacesStreamsAdapterClient.SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON);

        assertNotNull(transformed);
        assertInstanceOf(software.amazon.awssdk.services.kinesis.model.AccessDeniedException.class, transformed);
    }

    @Test
    void testTransformDescribeStreamValidationInvalidFormat() {
        ValidationException original = createValidationException(
                ValidationExceptionType.INVALID_FORMAT, TEST_ERROR_MESSAGE);

        AwsServiceException transformed = AmazonServiceExceptionTransformer
                .transformKeyspacesStreamsToKinesisDescribeStream(original,
                        AmazonKeyspacesStreamsAdapterClient.SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON);

        assertNotNull(transformed);
        assertInstanceOf(InvalidArgumentException.class, transformed);
    }

    @Test
    void testTransformGetRecordsInternalServerError() {
        InternalServerException original = createException(
                InternalServerException.builder(), TEST_ERROR_MESSAGE).build();

        AwsServiceException transformed = AmazonServiceExceptionTransformer
                .transformKeyspacesStreamsToKinesisGetRecords(original,
                        AmazonKeyspacesStreamsAdapterClient.SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON);

        assertNotNull(transformed);
        assertEquals(original.statusCode(), transformed.statusCode());
    }

    @Test
    void testTransformGetRecordsResourceNotFound() {
        ResourceNotFoundException original = createException(
                ResourceNotFoundException.builder(), TEST_ERROR_MESSAGE).build();

        AwsServiceException transformed = AmazonServiceExceptionTransformer
                .transformKeyspacesStreamsToKinesisGetRecords(original,
                        AmazonKeyspacesStreamsAdapterClient.SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON);

        assertNotNull(transformed);
        assertInstanceOf(software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException.class, transformed);
    }

    @Test
    void testTransformGetRecordsThrottling() {
        ThrottlingException original = createException(
                ThrottlingException.builder(), TEST_ERROR_MESSAGE).build();

        AwsServiceException transformed = AmazonServiceExceptionTransformer
                .transformKeyspacesStreamsToKinesisGetRecords(original,
                        AmazonKeyspacesStreamsAdapterClient.SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON);

        assertNotNull(transformed);
        assertInstanceOf(ProvisionedThroughputExceededException.class, transformed);
    }

    @Test
    void testTransformGetRecordsValidationExpiredIterator() {
        ValidationException original = createValidationException(
                ValidationExceptionType.EXPIRED_ITERATOR, TEST_ERROR_MESSAGE);

        AwsServiceException transformed = AmazonServiceExceptionTransformer
                .transformKeyspacesStreamsToKinesisGetRecords(original,
                        AmazonKeyspacesStreamsAdapterClient.SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON);

        assertNotNull(transformed);
        assertInstanceOf(ExpiredIteratorException.class, transformed);
    }

    @Test
    void testTransformGetRecordsValidationExpiredNextToken() {
        ValidationException original = createValidationException(
                ValidationExceptionType.EXPIRED_NEXT_TOKEN, TEST_ERROR_MESSAGE);

        AwsServiceException transformed = AmazonServiceExceptionTransformer
                .transformKeyspacesStreamsToKinesisGetRecords(original,
                        AmazonKeyspacesStreamsAdapterClient.SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON);

        assertNotNull(transformed);
        assertInstanceOf(ExpiredNextTokenException.class, transformed);
    }

    @Test
    void testTransformGetRecordsTrimmedDataWithSkip() {
        ValidationException original = createValidationException(
                ValidationExceptionType.TRIMMED_DATA_ACCESS, TEST_ERROR_MESSAGE);

        AwsServiceException transformed = AmazonServiceExceptionTransformer
                .transformKeyspacesStreamsToKinesisGetRecords(original,
                        AmazonKeyspacesStreamsAdapterClient.SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON);

        assertNotNull(transformed);
        assertInstanceOf(ExpiredIteratorException.class, transformed);
    }

    @Test
    void testTransformGetRecordsTrimmedDataWithRetry() {
        ValidationException original = createValidationException(
                ValidationExceptionType.TRIMMED_DATA_ACCESS, TEST_ERROR_MESSAGE);

        assertThrows(UnableToReadMoreRecordsException.class,
                () -> AmazonServiceExceptionTransformer.transformKeyspacesStreamsToKinesisGetRecords(original,
                        AmazonKeyspacesStreamsAdapterClient.SkipRecordsBehavior.KCL_RETRY));
    }

    @Test
    void testTransformGetShardIteratorInternalServerError() {
        InternalServerException original = createException(
                InternalServerException.builder(), TEST_ERROR_MESSAGE).build();

        AwsServiceException transformed = AmazonServiceExceptionTransformer
                .transformKeyspacesStreamsToKinesisGetShardIterator(original,
                        AmazonKeyspacesStreamsAdapterClient.SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON);

        assertNotNull(transformed);
        assertEquals(original.statusCode(), transformed.statusCode());
    }

    @Test
    void testTransformGetShardIteratorResourceNotFoundWithSkip() {
        ResourceNotFoundException original = createException(
                ResourceNotFoundException.builder(), TEST_ERROR_MESSAGE).build();

        AwsServiceException transformed = AmazonServiceExceptionTransformer
                .transformKeyspacesStreamsToKinesisGetShardIterator(original,
                        AmazonKeyspacesStreamsAdapterClient.SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON);

        assertNotNull(transformed);
        assertInstanceOf(software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException.class, transformed);
    }

    @Test
    void testTransformGetShardIteratorResourceNotFoundWithRetry() {
        ResourceNotFoundException original = createException(
                ResourceNotFoundException.builder(), TEST_ERROR_MESSAGE).build();

        assertThrows(UnableToReadMoreRecordsException.class,
                () -> AmazonServiceExceptionTransformer.transformKeyspacesStreamsToKinesisGetShardIterator(original,
                        AmazonKeyspacesStreamsAdapterClient.SkipRecordsBehavior.KCL_RETRY));
    }

    @Test
    void testTransformGetShardIteratorTrimmedDataWithSkip() {
        ValidationException original = createValidationException(
                ValidationExceptionType.TRIMMED_DATA_ACCESS, TEST_ERROR_MESSAGE);

        AwsServiceException transformed = AmazonServiceExceptionTransformer
                .transformKeyspacesStreamsToKinesisGetShardIterator(original,
                        AmazonKeyspacesStreamsAdapterClient.SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON);

        assertNotNull(transformed);
        assertInstanceOf(software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException.class, transformed);
    }

    @Test
    void testTransformGetShardIteratorTrimmedDataWithRetry() {
        ValidationException original = createValidationException(
                ValidationExceptionType.TRIMMED_DATA_ACCESS, TEST_ERROR_MESSAGE);

        assertThrows(UnableToReadMoreRecordsException.class,
                () -> AmazonServiceExceptionTransformer.transformKeyspacesStreamsToKinesisGetShardIterator(original,
                        AmazonKeyspacesStreamsAdapterClient.SkipRecordsBehavior.KCL_RETRY));
    }

    @Test
    void testTransformListStreamsInternalServerError() {
        InternalServerException original = createException(
                InternalServerException.builder(), TEST_ERROR_MESSAGE).build();

        AwsServiceException transformed = AmazonServiceExceptionTransformer
                .transformKeyspacesStreamsToKinesisListStreams(original,
                        AmazonKeyspacesStreamsAdapterClient.SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON);

        assertNotNull(transformed);
        assertEquals(original.statusCode(), transformed.statusCode());
    }

    @Test
    void testTransformListStreamsResourceNotFound() {
        ResourceNotFoundException original = createException(
                ResourceNotFoundException.builder(), TEST_ERROR_MESSAGE).build();

        AwsServiceException transformed = AmazonServiceExceptionTransformer
                .transformKeyspacesStreamsToKinesisListStreams(original,
                        AmazonKeyspacesStreamsAdapterClient.SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON);

        assertNotNull(transformed);
    }

    @Test
    void testTransformListStreamsThrottling() {
        ThrottlingException original = createException(
                ThrottlingException.builder(), TEST_ERROR_MESSAGE).build();

        AwsServiceException transformed = AmazonServiceExceptionTransformer
                .transformKeyspacesStreamsToKinesisListStreams(original,
                        AmazonKeyspacesStreamsAdapterClient.SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON);

        assertNotNull(transformed);
        assertInstanceOf(LimitExceededException.class, transformed);
    }

    @Test
    void testTransformListStreamsAccessDenied() {
        AccessDeniedException original = createException(
                AccessDeniedException.builder(), TEST_ERROR_MESSAGE).build();

        AwsServiceException transformed = AmazonServiceExceptionTransformer
                .transformKeyspacesStreamsToKinesisListStreams(original,
                        AmazonKeyspacesStreamsAdapterClient.SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON);

        assertNotNull(transformed);
        assertInstanceOf(software.amazon.awssdk.services.kinesis.model.AccessDeniedException.class, transformed);
    }

    @Test
    void testNullExceptionHandling() {
        AwsServiceException transformed = AmazonServiceExceptionTransformer
                .transformKeyspacesStreamsToKinesisDescribeStream(null,
                        AmazonKeyspacesStreamsAdapterClient.SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON);

        assertEquals(null, transformed);
    }

    private <T extends AwsServiceException.Builder> T createException(T builder, String message) {
        builder.message(message)
                .requestId(TEST_REQUEST_ID)
                .statusCode(500)
                .awsErrorDetails(AwsErrorDetails.builder()
                        .errorMessage(message)
                        .serviceName("Keyspaces")
                        .build());
        return builder;
    }

    private ValidationException createValidationException(ValidationExceptionType errorCode, String message) {
        return ValidationException.builder()
                .message(message)
                .requestId(TEST_REQUEST_ID)
                .statusCode(400)
                .errorCode(errorCode)
                .awsErrorDetails(AwsErrorDetails.builder()
                        .errorMessage(message)
                        .serviceName("Keyspaces")
                        .build())
                .build();
    }
}
