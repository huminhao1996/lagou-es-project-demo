package com.lagou.es.service.impl;

import com.lagou.es.service.PositionService;
import com.lagou.es.util.DBHelper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;


@Service
public class PositionServiceImpl implements PositionService {
    private static final Logger logger = LogManager.getLogger(PositionServiceImpl.class);
    @Autowired
    private RestHighLevelClient client;

    private static final String POSITION_INDEX = "position";

    @Override
    public List<Map<String, Object>> searchPos(String keyword, int pageNo, int pageSize) throws IOException {
        if (pageNo <= 1) {
            pageNo = 1;
        }

        // ??????
        SearchRequest searchRequest = new SearchRequest(POSITION_INDEX);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // ????????????
        searchSourceBuilder.from((pageNo - 1) * pageSize);
        searchSourceBuilder.size(pageSize);

        QueryBuilder builder = QueryBuilders.queryStringQuery(keyword)
                .field("companyName")
                .field("positionAdvantage")
                .field("positionName")
                .field("salary")
                .field("education")
                .field("workYear")
                .field("jobNature")
                .field("city")
                .defaultOperator(Operator.OR);

        searchSourceBuilder.query(builder);
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        //????????????
        HighlightBuilder highlighter = new HighlightBuilder();
        //???????????????
        highlighter.field("companyName")
                .field("positionAdvantage")
                .field("positionName")
                .field("salary")
                .field("education")
                .field("workYear")
                .field("jobNature")
                .field("city");
        highlighter.preTags("<font color='red'>");
        highlighter.postTags("</font>");
        searchSourceBuilder.highlighter(highlighter);

        // ????????????
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        ArrayList<Map<String, Object>> list = new ArrayList<>();
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();

            // ???????????????????????????goods??????title
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            for (Map.Entry<String, HighlightField> entry : highlightFields.entrySet()) {
                String key = entry.getKey();
                HighlightField HighlightField = entry.getValue();
                Text[] fragments = HighlightField.fragments();
                //??????
                sourceAsMap.put(key,fragments[0].toString());
            }
            list.add(sourceAsMap);
        }

        return list;
    }

    @Override
    public void importAll() throws IOException {
        writeMySQLDataToES("position");
    }

    /**
     * mysql -> es
     *
     * @param tableName
     */
    private void writeMySQLDataToES(String tableName) {
        //
        BulkProcessor bulkProcessor = getBulkProcessor(client);

        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            connection = DBHelper.getConn();
            logger.info("start handle data :" + tableName);
            String sql = "select * from " + tableName;
            ps = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            // ???????????????????????? fetchSize
            ps.setFetchSize(20);
            rs = ps.executeQuery();
            ResultSetMetaData colData = rs.getMetaData();
            ArrayList<HashMap<String, String>> dataList = new ArrayList<>();

            HashMap<String, String> map = null;
            int count = 0;
            // c ??????????????????   v ?????????????????????
            String c = null;
            String v = null;
            while (rs.next()) {
                count++;
                map = new HashMap<String, String>(128);
                for (int i = 1; i < colData.getColumnCount(); i++) {
                    c = colData.getColumnName(i);
                    v = rs.getString(c);
                    map.put(c, v);
                }
                dataList.add(map);
                // ???1?????? ?????????   ???????????????????????? ????????????????????????
                if (count % 10000 == 0) {
                    logger.info("mysql handle data  number:" + count);
                    // ?????????????????? bulkProcessor
                    for (HashMap<String, String> hashMap2 : dataList) {
                        bulkProcessor.add(new IndexRequest(POSITION_INDEX,"text").source(hashMap2));
                    }
                    // ??????????????? ?????? map ???  dataList
                    map.clear();
                    dataList.clear();
                }
            }

            // ?????? ??????????????????
            for (HashMap<String, String> hashMap2 : dataList) {
                bulkProcessor.add(new IndexRequest(POSITION_INDEX,"text").source(hashMap2));
            }
            // ??????????????????es, ?????????????????????????????????????????????????????????bulkProcessor????????? ????????????
            bulkProcessor.flush();

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                rs.close();
                ps.close();
                connection.close();
                boolean terinaFlag = bulkProcessor.awaitClose(150L, TimeUnit.SECONDS);
                logger.info(terinaFlag);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * bulk ????????????
     * @param client
     * @return
     */
    private BulkProcessor getBulkProcessor(RestHighLevelClient client) {

        BulkProcessor bulkProcessor = null;
        try {

            BulkProcessor.Listener listener = new BulkProcessor.Listener() {
                @Override
                public void beforeBulk(long executionId, BulkRequest request) {
                    logger.info("Try to insert data number : "
                            + request.numberOfActions());
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request,
                                      BulkResponse response) {
                    logger.info("************** Success insert data number : "
                            + request.numberOfActions() + " , id: " + executionId);
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                    logger.error("Bulk is unsuccess : " + failure + ", executionId: " + executionId);
                }
            };

            BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulkConsumer = (request, bulkListener) -> client
                    .bulkAsync(request, RequestOptions.DEFAULT, bulkListener);

            BulkProcessor.Builder builder = BulkProcessor.builder(bulkConsumer, listener);
            builder.setBulkActions(5000);
            builder.setBulkSize(new ByteSizeValue(100L, ByteSizeUnit.MB));
            builder.setConcurrentRequests(10);
            builder.setFlushInterval(TimeValue.timeValueSeconds(100L));
            builder.setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(1L), 3));
            // ?????????????????????????????????
            bulkProcessor = builder.build();

        } catch (Exception e) {
            e.printStackTrace();
            try {
                bulkProcessor.awaitClose(100L, TimeUnit.SECONDS);
            } catch (Exception e1) {
                logger.error(e1.getMessage());
            }
        }
        return bulkProcessor;
    }
}

