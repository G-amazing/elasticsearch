package com.elasticsearch.demo;

import com.elasticsearch.entity.Goods;
import com.elasticsearch.repository.GoodRepository;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.query.FetchSourceFilter;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.test.context.junit4.SpringRunner;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@RunWith(SpringRunner.class)
@SpringBootTest
public class DemoApplicationTests {
    @Autowired
    private GoodRepository goodRepository;
    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;

    // 创建索引
    @Test
    public void testCreate() {
        // 创建索引，会根据Item类的@Document注解信息来创建
        elasticsearchTemplate.createIndex(Goods.class);
        // 配置映射，会根据Item类中的id、Field等字段来自动完成映射
        elasticsearchTemplate.putMapping(Goods.class);
    }

    // 新增文档 (当id相同时覆盖原来的数据)
    @Test
    public void index() {
        Goods g = new Goods(1L, "小米手机", 2999.3, "小米");
        goodRepository.save(g);
    }

    // 批量增加文档 (当id相同时覆盖原来的数据)
    @Test
    public void indexList() {
        List<Goods> goodsList = new ArrayList<>();
        goodsList.add(new Goods(2L, "apple手机", 6288.0, "apple"));
        goodsList.add(new Goods(3L, "华为手机", 3695.8, "华为"));
        goodsList.add(new Goods(4L, "OPPO手机", 4633.3, "OPPO"));
        goodsList.add(new Goods(5L, "华为手机", 1999.4, "华为"));
        goodsList.add(new Goods(6L, "小米电视", 4888.8, "小米"));
        goodRepository.saveAll(goodsList);
    }

    // 查询单个
    @Test
    public void testQuery() {
        Optional<Goods> g = goodRepository.findById(1L);
        System.out.println(g);
    }

    // 查询全部，并按照价格降序排序
    @Test
    public void testFind(){
        Iterable<Goods> items = goodRepository.findAll(Sort.by(Sort.Direction.DESC, "price"));
        items.forEach(System.out::println);
    }

    // 根据自定义的方法查询，价格在 2000 - 4000 的手机
    @Test
    public void queryByPriceBetween() {
        List<Goods> list = goodRepository.findByPriceBetween(2000.00, 4000.00);
        list.forEach(System.out::println);
    }

    // =============================高级查询===========================

    // 词条查询
    @Test
    public void testQueryByTitle() {
        // 设置词条
        MatchQueryBuilder queryBuilder = QueryBuilders.matchQuery("title", "小米");
        // 执行查询
        Iterable<Goods> list = goodRepository.search(queryBuilder);
        // 打印结果集
        list.forEach(System.out::println);
    }

    // 自定义查询
    @Test
    public void testNativeQuery() {
        // 构建查询条件
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        // 添加基本的分词查询
        queryBuilder.withQuery(QueryBuilders.matchQuery("title", "小米"));
        // 执行搜索，获取结果
        Page<Goods> list = goodRepository.search(queryBuilder.build());
        // 打印总条数
        System.out.println(list.getTotalElements());
        // 打印总页数
        System.out.println(list.getTotalPages());
        // 打印结果集
        list.forEach(System.out::println);
    }

    // 分页查询
    @Test
    public void testNativeQueryPage(){
        // 构建查询条件
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        // 添加基本的分词查询
        queryBuilder.withQuery(QueryBuilders.termQuery("title", "手机"));

        // 初始化分页参数   从 0 开始算第一页！！！
        int page = 0;
        int size = 3;
        // 设置分页参数
        queryBuilder.withPageable(PageRequest.of(page, size));

        // 执行搜索，获取结果
        Page<Goods> list = goodRepository.search(queryBuilder.build());
        // 打印总条数
        System.out.println("总条数: " + list.getTotalElements());
        // 打印总页数
        System.out.println("总页数: " + list.getTotalPages());
        // 每页大小
        System.out.println("每页大小: " + list.getSize());
        // 当前页
        System.out.println("当前页: " + list.getNumber());
        // 打印结果集
        list.forEach(System.out::println);
    }

    // 排序
    @Test
    public void testSort(){
        // 构建查询条件
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        // 添加分词查询条件
        queryBuilder.withQuery(QueryBuilders.termQuery("title", "手机"));
        // 添加排序条件
        queryBuilder.withSort(SortBuilders.fieldSort("price").order(SortOrder.DESC));
        // 执行查询
        Page<Goods> list = goodRepository.search(queryBuilder.build());
        // 打印总条数
        System.out.println("总条数: " + list.getTotalElements());
        // 打印结果集
        list.forEach(System.out::println);
    }

    // =============================聚合=================================

    @Test
    public void testAgg(){
        // 构建查询条件
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        // 不查询任何结果
        queryBuilder.withSourceFilter(new FetchSourceFilter(new String[]{""}, null));
        // 1、添加一个新的聚合，聚合类型为terms(根据字段为聚合条件)，聚合名称为brands(下面要根据这个名称获取查询条件)，聚合字段为brand
        queryBuilder.addAggregation(
                AggregationBuilders.terms("brands").field("brand"));
        // 2、执行查询,需要把结果强转为AggregatedPage类型
        AggregatedPage<Goods> aggPage = (AggregatedPage<Goods>) goodRepository.search(queryBuilder.build());
        // 3、解析
        // 3.1、从结果中取出名为brands的那个聚合，
        // 因为是利用String类型字段来进行的term聚合，所以结果要强转为StringTerm类型
        StringTerms agg = (StringTerms) aggPage.getAggregation("brands");
        // 3.2、获取桶
        List<StringTerms.Bucket> buckets = agg.getBuckets();
        // 3.3、遍历
        for (StringTerms.Bucket bucket : buckets) {
            // 3.4、获取桶中的key，即品牌名称
            System.out.println(bucket.getKeyAsString());
            // 3.5、获取桶中的文档数量
            System.out.println(bucket.getDocCount());
        }

    }

}
