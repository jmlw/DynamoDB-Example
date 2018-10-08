package com.joshmlwood.dynamoexample;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DocumentInterface {
    private ObjectMapper objectMapper;
    private AmazonDynamoDB amazonDynamoDB;
    private String hash = "Hash";
    private String range = "Range";

    public DocumentInterface(ObjectMapper objectMapper, AmazonDynamoDB amazonDynamoDB) {
        this.objectMapper = objectMapper;
        this.amazonDynamoDB = amazonDynamoDB;
    }
}
