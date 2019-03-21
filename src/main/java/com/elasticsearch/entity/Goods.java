package com.elasticsearch.entity;

import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

@Document(indexName = "good",type = "docs", shards = 1, replicas = 0)
public class Goods {
    @Id
    private Long id;
    @Field(type = FieldType.Text, analyzer = "ik_max_word")
    private String title; //标题
    @Field(type = FieldType.Double)
    private Double price; // 价格
    @Field(type = FieldType.Keyword)
    private String brand; // 品牌

    public Goods(Long id, String title, Double price) {
        this.id = id;
        this.title = title;
        this.price = price;
    }

    public Goods(Long id, String title, Double price, String brand) {
        this.id = id;
        this.title = title;
        this.price = price;
        this.brand = brand;
    }

    public Goods() {
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public String getBrand() {
        return brand;
    }

    public void setBrand(String brand) {
        this.brand = brand;
    }

    @Override
    public String toString() {
        return "Goods{" +
                "id=" + id +
                ", title='" + title + '\'' +
                ", price=" + price +
                '}';
    }
}
