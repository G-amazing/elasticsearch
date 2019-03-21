package com.elasticsearch.repository;

import com.elasticsearch.entity.Goods;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

import java.util.List;

public interface GoodRepository extends ElasticsearchRepository<Goods, Long> {
    List<Goods> findByPriceBetween(double p1, double p2);
}
