package mao.elasticsearch_create_index;

import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Project name(项目名称)：elasticsearch_create_Index
 * Package(包名): mao.elasticsearch_create_index
 * Class(类名): ElasticSearchTest
 * Author(作者）: mao
 * Author QQ：1296193245
 * GitHub：https://github.com/maomao124/
 * Date(创建日期)： 2022/5/27
 * Time(创建时间)： 9:59
 * Version(版本): 1.0
 * Description(描述)： SpringBootTest
 */

@SpringBootTest
public class ElasticSearchTest
{
    private static RestHighLevelClient client;

    @BeforeAll
    static void beforeAll()
    {
        client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("localhost", 9200, "http")));
    }

    //创建索引：
    //PUT /index
    //{
    //    "settings": {},
    //    "mappings": {
    //       "properties" : {
    //            "field1" : { "type" : "text" }
    //        }
    //    },
    //    "aliases": {
    //    	"default_index": {}
    //  }
    //}

    /**
     * 创建索引
     * 方法1
     *
     * @throws IOException IOException
     */
    @Test
    void create_index() throws IOException
    {
        //构建请求
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("my_index");
        //设置参数settings
        createIndexRequest.settings();
        //设置映射mappings

        //方式1
        createIndexRequest.mapping("{\n" +
                "               \"dynamic\": \"strict\"," +
                "              \"properties\" : {\n" +
                "                   \"name\" : { \"type\" : \"text\" },\n" +
                "                   \"name2\" : { \"type\" : \"text\" }\n" +
                "              }}", XContentType.JSON);

        //设置别名aliases
        //createIndexRequest.alias(new Alias("my_index2"));

        //操作索引的客户端
        IndicesClient indices = client.indices();
        //发起请求
        CreateIndexResponse createIndexResponse = indices.create(createIndexRequest, RequestOptions.DEFAULT);
        //获得数据
        boolean acknowledged = createIndexResponse.isAcknowledged();
        System.out.println(acknowledged);
    }

    /**
     * 创建索引
     * 方法2
     *
     * @throws IOException IOException
     */
    @Test
    void create_index2() throws IOException
    {
        //构建请求
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("my_index");
        //设置参数settings
        createIndexRequest.settings();
        //设置映射mappings

        //方式2
        Map<String, Object> name = new HashMap<>();
        name.put("type", "text");
        Map<String, Object> name2 = new HashMap<>();
        name2.put("type", "text");
        Map<String, Object> properties = new HashMap<>();
        properties.put("name", name);
        properties.put("name2", name2);
        Map<String, Object> map = new HashMap<>();
        map.put("dynamic", "strict");
        map.put("properties", properties);
        createIndexRequest.mapping(map);


        //操作索引的客户端
        IndicesClient indices = client.indices();
        //发起请求
        CreateIndexResponse createIndexResponse = indices.create(createIndexRequest, RequestOptions.DEFAULT);
        //获得数据
        boolean acknowledged = createIndexResponse.isAcknowledged();
        System.out.println(acknowledged);
    }

    /**
     * 创建索引
     * 方法3
     *
     * @throws IOException IOException
     */
    @Test
    void create_index3() throws IOException
    {
        //构建请求
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("my_index");
        //设置参数settings
        createIndexRequest.settings();
        //设置映射mappings

        //方式3
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        xContentBuilder.startObject();
        {
            xContentBuilder.field("dynamic", "strict");
            xContentBuilder.startObject("properties");
            {
                xContentBuilder.startObject("name");
                {
                    xContentBuilder.field("type", "text");
                }
                xContentBuilder.endObject();

                xContentBuilder.startObject("name2");
                {
                    xContentBuilder.field("type", "text");
                }
                xContentBuilder.endObject();
            }
            xContentBuilder.endObject();
        }
        xContentBuilder.endObject();

        createIndexRequest.mapping(xContentBuilder);


        //操作索引的客户端
        IndicesClient indices = client.indices();
        //发起请求
        CreateIndexResponse createIndexResponse = indices.create(createIndexRequest, RequestOptions.DEFAULT);
        //获得数据
        boolean acknowledged = createIndexResponse.isAcknowledged();
        System.out.println(acknowledged);
    }

    @Test
    void create_index_async() throws IOException
    {
        //构建请求
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("my_index");
        //设置参数settings
        createIndexRequest.settings();
        //设置映射mappings

        //方式3
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        xContentBuilder.startObject();
        {
            xContentBuilder.field("dynamic", "strict");
            xContentBuilder.startObject("properties");
            {
                xContentBuilder.startObject("name");
                {
                    xContentBuilder.field("type", "text");
                }
                xContentBuilder.endObject();

                xContentBuilder.startObject("name2");
                {
                    xContentBuilder.field("type", "text");
                }
                xContentBuilder.endObject();
            }
            xContentBuilder.endObject();
        }
        xContentBuilder.endObject();

        createIndexRequest.mapping(xContentBuilder);

        IndicesClient indices = client.indices();
        //发起异步请求
        indices.createAsync(createIndexRequest, RequestOptions.DEFAULT, new ActionListener<CreateIndexResponse>()
        {
            @Override
            public void onResponse(CreateIndexResponse createIndexResponse)
            {
                //获得数据
                boolean acknowledged = createIndexResponse.isAcknowledged();
                System.out.println(acknowledged);
            }

            @Override
            public void onFailure(Exception e)
            {
                e.printStackTrace();
            }
        });
        //休眠
        try
        {
            Thread.sleep(2000);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }
}
