package com.wadhara.aws;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;

public class AwsKinesisClient {

    public static final String AWS_ACCESS_KEY = "aws.accessKeyId";
    public static final String AWS_SECRET_KEY = "aws.secretKey";

    static {
        //add your AWS account access key and secret key
        System.setProperty(AWS_ACCESS_KEY, "AKIAV7ILO752Q6IDYKUG");
        System.setProperty(AWS_SECRET_KEY, "oqtxy7VtJGYIlgIe4nQ+Q37Dk88K+Y3/uvUTcDmu");
    }

    public static AmazonKinesis getKinesisClient(){
        return AmazonKinesisClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .build();
    }
}
