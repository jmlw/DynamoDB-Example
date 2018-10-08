package com.joshmlwood.dynamoexample;

public class MyPojo {
    private Integer id;
    private String someData;

    public MyPojo() {
        // default constructor for Jackson
    }

    public MyPojo(Integer id, String someData) {
        this.id = id;
        this.someData = someData;
    }

    public Integer getId() {
        return id;
    }

    public String getSomeData() {
        return someData;
    }
}
