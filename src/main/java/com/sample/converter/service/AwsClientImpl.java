package com.sample.converter.service;

import java.io.InputStream;
import java.util.Map;

import org.apache.commons.text.StringSubstitutor;
import org.springframework.stereotype.Component;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class AwsClientImpl {

    public void uploadToS3(Map<String, String> chronoMap, String fileName,
            InputStream contentStream) {
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.addUserMetadata("transmitMessages", "False");
        objectMetadata.setContentType("binary/octet-stream");
        String s3Urinew = "s3://poc-test-123/mytest/{day}";
        AmazonS3URI s3Uri = new AmazonS3URI(s3Urinew);
        String objectKey = StringSubstitutor.replace(s3Uri.getKey(), chronoMap, "{", "}")
                .concat(fileName);
        PutObjectRequest putObjectRequest = new PutObjectRequest(s3Uri.getBucket(), objectKey, contentStream,
                objectMetadata);

        AmazonS3 s3Client = this.buildAmazonS3Client();

        try {
            s3Client.putObject(putObjectRequest);
        } catch (AmazonServiceException ase) {

            String reason = String.format(
                "Encountered AmazonServiceException: \n"
                        + "HTTP Status Code: %d \n"
                        + "Error Message: %s \n"
                        + "AWS Error Code: %s /n"
                        + "Error Type: %s \n"
                        + "Request ID: %s \n",
                ase.getStatusCode(),
                ase.getMessage(),
                ase.getErrorCode(),
                ase.getErrorType().toString(),
                ase.getRequestId());
            log.error(reason, ase);
        } catch (AmazonClientException ace) {
            log.error("Encountered AmazonClientException: ", ace);
        }
    }

    public AmazonS3 buildAmazonS3Client() {

        String accessKeyId = "AKIA4VUBKMI7GQWP3EAN";
        String secretAccessKey = "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";
        return AmazonS3ClientBuilder.standard().withRegion(Regions.EU_CENTRAL_1)
                .withCredentials(
                    new AWSStaticCredentialsProvider(new BasicAWSCredentials(
                            accessKeyId,
                            secretAccessKey)))
                .build();
    }
}
