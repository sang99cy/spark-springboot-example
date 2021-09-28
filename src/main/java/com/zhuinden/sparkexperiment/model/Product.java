package com.zhuinden.sparkexperiment.model;

public class Product {
    private String id;
    private String name;
    private String price;
    private String type;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPrice() {
        return price;
    }

    public void setPrice(String price) {
        this.price = price;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Product() {
    }

    public Product(String id, String name, String price, String type) {
        this.id = id;
        this.name = name;
        this.price = price;
        this.type = type;
    }
}
