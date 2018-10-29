package com.joshmlwood.dynamoexample;

import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Main {
    private static ObjectMapper objectMapper;

    public static void main(String[] args) {
        objectMapper = new ObjectMapper();
        AmazonDynamoDB amazonDynamoDB = new AmazonDynamoDBClient(new AWSCredentialsProviderChain(new ProfileCredentialsProvider()));
        LowLevelApi lowLevelApi = new LowLevelApi(objectMapper, amazonDynamoDB);

        lowLevelApi.find("TestTable", "hash", "My data");
        lowLevelApi.save("TestTable", "one", "one-range", new MyPojo(1, "yadda yadda"));
        tryPrint(lowLevelApi.getByKeys("TestTable", "one", "one-range"));
        tryPrint(lowLevelApi.find("TestTable", "one", "yadda"));
        tryPrint(lowLevelApi.getAll("TestTable"));

        lowLevelApi.delete("TestTable", "one", "one-range");
        tryPrint(lowLevelApi.getAll("TestTable"));
    }

    private static void tryPrint(Object o) {
        try {
            String serialized = objectMapper.writeValueAsString(o);
            System.out.println(serialized);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
}
