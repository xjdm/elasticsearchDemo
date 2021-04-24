package com.idstaa;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author chenjie
 * @date 2021/4/24 14:19
 */
@SpringBootTest
@RunWith(SpringRunner.class)
public class es {
    @Autowired
    RestHighLevelClient client;

    @Test
    public void testCreateIndex() throws IOException {
        // 创建一个索引请求对象
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("elasticsearch_test");
        // 设置映射
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field("properties").startObject()
                .field("description").startObject()
                .field("type", "text")
                .field("analyzer", "ik_max_word").endObject()
                .field("name").startObject().field("type", "keyword").endObject()
                .field("pic").startObject().field("type", "text").field("index", "false").endObject()
                .field("studymodel").startObject().field("type", "keyword").endObject()
                .endObject()
                .endObject();
        createIndexRequest.mapping("doc", xContentBuilder);
        // 操作索引的客户端
        IndicesClient indicesClient = client.indices();
        CreateIndexResponse createIndexResponse = indicesClient.create(createIndexRequest, RequestOptions.DEFAULT);
        // 得到响应
        boolean acknowledged = createIndexResponse.isAcknowledged();
        System.out.println(acknowledged);


    }

    //删除索引库
    @Test
    public void testDeleteIndex() throws IOException {
        //删除索引的请求对象
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("elasticsearch_test");
        // 操作索引的客户端
        IndicesClient indices = client.indices();
        // 执行删除索引
        AcknowledgedResponse delete = indices.delete(deleteIndexRequest, RequestOptions.DEFAULT);
        // 得到响应
        boolean acknowledged = delete.isAcknowledged();
        System.out.println(acknowledged);
    }

    @Test
    public void testAddDoc() throws IOException {
        //创建索引请求对象
        // IndexRequest indexRequest = new IndexRequest("elasticsearch_test", "doc");
        IndexRequest indexRequest = new IndexRequest("elasticsearch_test");
        indexRequest.id("2");
        //文档内容 准备json数据
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("name", "spring cloud实战");
        jsonMap.put("description", "本课程主要从四个章节进行讲解： 1.微服务架构入门 2.spring cloud 基础入门 3.实战Spring Boot 4.注册中心eureka。");
        jsonMap.put("studymodel", "201001");
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        jsonMap.put("timestamp", dateFormat.format(new Date()));
        jsonMap.put("price", 5.6f);
        indexRequest.source(jsonMap);
        //通过client进行http的请求
        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        DocWriteResponse.Result result = indexResponse.getResult();
        System.out.println(result);
    }

    //查询文档
    @Test
    public void testGetDoc() throws IOException {
        //查询请求对象
        GetRequest getRequest = new GetRequest("elasticsearch_test", "2");
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        //得到文档的内容
        Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
        System.out.println(sourceAsMap);
    }

    @Test
    public void testSearchAll() throws IOException, ParseException, ParseException {
        //搜索请求对象
        SearchRequest searchRequest = new SearchRequest("elasticsearch_test");
        //搜索源构建对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //搜索方式
        // matchAllQuery搜索全部
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        //设置源字段过虑,第一个参数结果集包括哪些字段，第二个参数表示结果集不包括哪些字段
        searchSourceBuilder.fetchSource(new String[]{"name", "studymodel", "price", "timestamp"}, new String[]{});
        //向搜索请求对象中设置搜索源
        searchRequest.source(searchSourceBuilder);
        //执行搜索,向ES发起http请求
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        //搜索结果
        SearchHits hits = searchResponse.getHits();
        //匹配到的总记录数
        TotalHits totalHits = hits.getTotalHits();
        //得到匹配度高的文档
        SearchHit[] searchHits = hits.getHits();
        //日期格式化对象
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        for (SearchHit hit : searchHits) {
            //文档的主键
            String id = hit.getId();
            //源文档内容
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name = (String) sourceAsMap.get("name");
            //由于前边设置了源文档字段过虑，这时description是取不到的
            String description = (String) sourceAsMap.get("description");
            //学习模式
            String studymodel = (String) sourceAsMap.get("studymodel");
            //价格
            Double price = (Double) sourceAsMap.get("price");
            //日期
            Date timestamp = dateFormat.parse((String) sourceAsMap.get("timestamp"));
            System.out.println(name);
            System.out.println(studymodel);
            System.out.println(description);
        }
    }


    @Test
    public void testTermQuery() throws IOException, ParseException, ParseException {
        //搜索请求对象
        SearchRequest searchRequest = new SearchRequest("elasticsearch_test");
        //搜索源构建对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //搜索方式
        // matchAllQuery搜索全部
        searchSourceBuilder.query(QueryBuilders.termQuery("name", "spring cloud实战"));
        //设置源字段过虑,第一个参数结果集包括哪些字段，第二个参数表示结果集不包括哪些字段
        searchSourceBuilder.fetchSource(new String[]{"name", "studymodel", "price", "timestamp"}, new String[]{});
        //向搜索请求对象中设置搜索源
        searchRequest.source(searchSourceBuilder);
        //执行搜索,向ES发起http请求
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        //搜索结果
        SearchHits hits = searchResponse.getHits();
        //匹配到的总记录数
        TotalHits totalHits = hits.getTotalHits();
        //得到匹配度高的文档
        SearchHit[] searchHits = hits.getHits();
        //日期格式化对象
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        for (SearchHit hit : searchHits) {
            //文档的主键
            String id = hit.getId();
            //源文档内容
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name = (String) sourceAsMap.get("name");
            //由于前边设置了源文档字段过虑，这时description是取不到的
            String description = (String) sourceAsMap.get("description");
            //学习模式
            String studymodel = (String) sourceAsMap.get("studymodel");
            //价格
            Double price = (Double) sourceAsMap.get("price");
            //日期
            Date timestamp = dateFormat.parse((String) sourceAsMap.get("timestamp"));
            System.out.println(name);
            System.out.println(studymodel);
            System.out.println(description);
        }
    }

    @Test
    public void testSearchAllPage() throws IOException, ParseException, ParseException {
        //搜索请求对象
        SearchRequest searchRequest = new SearchRequest("elasticsearch_test");
        //搜索源构建对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //搜索方式
        // matchAllQuery搜索全部
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        //设置源字段过虑,第一个参数结果集包括哪些字段，第二个参数表示结果集不包括哪些字段
        searchSourceBuilder.fetchSource(new String[]{"name", "studymodel", "price", "timestamp"}, new String[]{});
        // 设置分页参数
        int page = 1;
        int size = 2;
        // 计算出from
        int from = (page - 1) * size;
        searchSourceBuilder.from(from);
        searchSourceBuilder.size(size);
        searchSourceBuilder.sort("name",SortOrder.DESC);


        //向搜索请求对象中设置搜索源
        searchRequest.source(searchSourceBuilder);
        //执行搜索,向ES发起http请求
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        //搜索结果
        SearchHits hits = searchResponse.getHits();
        //匹配到的总记录数
        TotalHits totalHits = hits.getTotalHits();
        //得到匹配度高的文档
        SearchHit[] searchHits = hits.getHits();
        //日期格式化对象
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println("查询到的总记录数：" + totalHits);
        for (SearchHit hit : searchHits) {
            //文档的主键
            String id = hit.getId();
            //源文档内容
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name = (String) sourceAsMap.get("name");
            //由于前边设置了源文档字段过虑，这时description是取不到的
            String description = (String) sourceAsMap.get("description");
            //学习模式
            String studymodel = (String) sourceAsMap.get("studymodel");
            //价格
            Double price = (Double) sourceAsMap.get("price");
            //日期
            Date timestamp = dateFormat.parse((String) sourceAsMap.get("timestamp"));
            System.out.println(name);
            System.out.println(studymodel);
            System.out.println(description);
        }
    }
}


