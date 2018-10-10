package com.joshmlwood.dynamoexample;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.internal.InternalUtils;
import com.amazonaws.services.dynamodbv2.model.*;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;
import java.util.stream.Collectors;

public class LowLevelApi {
    private ObjectMapper objectMapper;
    private AmazonDynamoDB amazonDynamoDB;
    private String hash = "Hash";
    private String range = "Range";

    public LowLevelApi(ObjectMapper objectMapper, AmazonDynamoDB amazonDynamoDB) {
        this.objectMapper = objectMapper;
        this.amazonDynamoDB = amazonDynamoDB;
    }

    public MyPojo getByKeys(String tableName, String hashKey, String rangeKey) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put(hash, new AttributeValue(hashKey));
        key.put(range, new AttributeValue(rangeKey));

        GetItemRequest request = new GetItemRequest(tableName, key);

        GetItemResult result = amazonDynamoDB.getItem(request);

        return Optional.of(result)
                .map(GetItemResult::getItem)
                .map(stringAttributeValueMap -> stringAttributeValueMap.get("documentData"))
                .flatMap(this::deserialize) //unpack optional
                .orElse(null);
    }

    public void save(String tableName, String hashKey, String rangeKey, MyPojo myPojo) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put(hash, new AttributeValue(hashKey));
        item.put(range, new AttributeValue(rangeKey));


        serialize(myPojo).ifPresent(serialized -> {
            item.put("documentData", new AttributeValue(serialized));
            PutItemRequest request = new PutItemRequest(tableName, item);

            amazonDynamoDB.putItem(request);
            InternalUtils.toSimpleMapValue(new HashMap<String, AttributeValue>());
            InternalUtils.fromSimpleMap(new HashMap<String, Object>());
        });
    }

    private Optional<String> serialize(MyPojo myPojo) {
        try {
            return Optional.of(objectMapper.writeValueAsString(myPojo));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    public void delete(String tableName, String hashKey, String rangeKey) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put(hash, new AttributeValue(hashKey));
        item.put(range, new AttributeValue(rangeKey));

        DeleteItemRequest request = new DeleteItemRequest(tableName, item);

        amazonDynamoDB.deleteItem(request);
    }

    public List<MyPojo> getAll(String tableName) {
        ScanRequest request = new ScanRequest(tableName);
        ScanResult result = amazonDynamoDB.scan(request);
        Map<String, AttributeValue> lastEvaluatedKey = result.getLastEvaluatedKey();

        List<MyPojo> results = new ArrayList<>(deserialize(result.getItems()));

        while (lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty()) {
            request.setExclusiveStartKey(lastEvaluatedKey);
            result = amazonDynamoDB.scan(request);
            lastEvaluatedKey = result.getLastEvaluatedKey();

            results.addAll(deserialize(result.getItems()));
        }
        return results;
    }

    private List<MyPojo> deserialize(List<Map<String, AttributeValue>> items) {
        return items.stream()
                .map(stringAttributeValueMap -> stringAttributeValueMap.get("documentData"))
                .map(this::deserialize)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
    }

    // see https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ConditionExpressions.html#ConditionExpressionReference
    public List<MyPojo> getAllSomeDataBeginsWith(String tableName, String someDataBeginsWith) {
        ScanRequest request = new ScanRequest(tableName);
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":someData_queryString", new AttributeValue(someDataBeginsWith));

        String filterExpression = "begins_with(documentData.someData, :someData_queryString)";
        request.withFilterExpression(filterExpression);
        request.withExpressionAttributeValues(expressionAttributeValues);

        ScanResult result = amazonDynamoDB.scan(request);
        Map<String, AttributeValue> lastEvaluatedKey = result.getLastEvaluatedKey();

        List<MyPojo> results = new ArrayList<>(deserialize(result.getItems()));

        while (lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty()) {
            request.setExclusiveStartKey(lastEvaluatedKey);
            result = amazonDynamoDB.scan(request);
            lastEvaluatedKey = result.getLastEvaluatedKey();

            results.addAll(deserialize(result.getItems()));
        }
        return results;
    }

    public List<MyPojo> find(String tableName,
                             String hashKey,
//                             String rangeKeyStart,
//                             String rangeKeyEnd,
                             String someDataBeginsWith) {
        QueryRequest request = new QueryRequest(tableName);
        Map<String, Condition> keyConditionExpression = new HashMap<>();
        keyConditionExpression.put(hash, new Condition()
                .withComparisonOperator(ComparisonOperator.EQ)
                .withAttributeValueList(new AttributeValue(hashKey)));
//        keyConditionExpression.put(range, new Condition()
//                .withComparisonOperator(ComparisonOperator.BETWEEN)
//                .withAttributeValueList(new AttributeValue(rangeKeyStart), new AttributeValue(rangeKeyEnd)));
        String filterExpression = "begins_with(documentData.someData, :someData_queryString)";
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":someData_queryString", new AttributeValue(someDataBeginsWith));

        request.withKeyConditions(keyConditionExpression);
        request.withFilterExpression(filterExpression);
        request.withExpressionAttributeValues(expressionAttributeValues);

        QueryResult result = amazonDynamoDB.query(request);
        Map<String, AttributeValue> lastEvaluatedKey = result.getLastEvaluatedKey();

        List<MyPojo> results = new ArrayList<>(deserialize(result.getItems()));

        while (lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty()) {
            request.setExclusiveStartKey(lastEvaluatedKey);
            result = amazonDynamoDB.query(request);
            lastEvaluatedKey = result.getLastEvaluatedKey();

            results.addAll(deserialize(result.getItems()));
        }
        return results;
    }

    private Optional<MyPojo> deserialize(AttributeValue data) {
        try {
            String json = data.getS();
            return Optional.of(objectMapper.readValue(json, MyPojo.class));
        } catch (Exception e) {
            e.printStackTrace();
            return Optional.empty();
        }
    }
}
