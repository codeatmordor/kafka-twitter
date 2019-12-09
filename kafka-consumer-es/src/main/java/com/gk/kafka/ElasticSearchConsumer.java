package com.gk.kafka;

import java.io.IOException;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchConsumer {

    public static RestHighLevelClient  createClient(){

       // https://68b3ulwtcw:j3wohl05ua@kafka-course-6530265977.ap-southeast-2.bonsaisearch.net:443
        String hostName="kafka-course-6530265977.ap-southeast-2.bonsaisearch.net";
        String userName = "68b3ulwtcw";
        String password = "j3wohl05ua";

        final CredentialsProvider  credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName,password));

        RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost(hostName,443,"https")).setHttpClientConfigCallback(
            new HttpClientConfigCallback() {
                @Override
                public HttpAsyncClientBuilder customizeHttpClient(
                    HttpAsyncClientBuilder httpAsyncClientBuilder) {
                    return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);

                }
            });

        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);
        return restHighLevelClient;
    }
    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());


        RestHighLevelClient client = createClient();

        IndexRequest indexRequest = new IndexRequest("twitter", "tweets").source("{\"foo\":\"bar\"}", XContentType.JSON);

        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);

        String id = indexResponse.getId();
        logger.info(id);
        client.close();



    }
}
