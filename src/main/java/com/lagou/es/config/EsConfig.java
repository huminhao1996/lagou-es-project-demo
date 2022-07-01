package com.lagou.es.config;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EsConfig {

    @Value("${spring.elasticsearch.rest.uris}")
    private  String  hostlist;

    @Value("${spring.elasticsearch.rest.username}")
    private  String  username;

    @Value("${spring.elasticsearch.rest.password}")
    private  String  password;

    @Bean
    public RestHighLevelClient client() {
        String auth = Base64.encodeBase64String((username + ":" + password).getBytes());

        //解析hostlist配置信息
        String[] split = hostlist.split(",");
        //创建HttpHost数组，其中存放es主机和端口的配置信息
        HttpHost[] httpHostArray = new HttpHost[split.length];
        for(int i=0;i<split.length;i++){
            String item = split[i];
            httpHostArray[i] = new HttpHost(item.split(":")[0], Integer.parseInt(item.split(":")[1]), "http");
        }

        for (HttpHost httpHost : httpHostArray) {
            System.out.println(httpHost);
        }

        // 设置用户名 密码
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,new UsernamePasswordCredentials(username,password));

        //创建RestHighLevelClient客户端
        RestClientBuilder builder = RestClient.builder(httpHostArray);
        builder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback(){
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
        });

//        builder.setDefaultHeaders(new BasicHeader[]{
//                new BasicHeader("Authorization","Basic " + auth)
//        });
        return new RestHighLevelClient(builder);
    }

}