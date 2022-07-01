package com.lagou.es.controller;

import com.lagou.es.service.PositionService;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Controller
public class PositionController {
    @Autowired
    private PositionService service;

    @Autowired
    private RestHighLevelClient client;

    private static final String POSITION_INDEX = "position";

    // 测试页面
    @GetMapping({"/", "/index"})
    public String indexPage() {
        return "index";
    }

    /**
     * 查询索引
     */
    @GetMapping("queryIndex")
    public void queryIndex() throws IOException {
        IndicesClient indices = client.indices();

        GetIndexRequest getReqeust = new GetIndexRequest();
        getReqeust.indices("movie");

        GetIndexResponse response = indices.get(getReqeust, RequestOptions.DEFAULT);

        //获取结果
//        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = response.getMappings();

//        for (String key : mappings.keySet()) {
//            System.out.println(key+":" + mappings.get(key).getSourceAsMap());
//        }

    }

    /**
     * es 搜索
     * @param keyword
     * @param pageNo
     * @param pageSize
     * @return
     * @throws IOException
     */
    @GetMapping("/search/{keyword}/{pageNo}/{pageSize}")
    @ResponseBody
    public List<Map<String, Object>> searchPosition(@PathVariable("keyword") String keyword,
                                                    @PathVariable("pageNo") int pageNo,
                                                    @PathVariable("pageSize") int pageSize) throws IOException {
        System.out.println("/search/{keyword}/{pageNo}/{pageSize}");
        List<Map<String, Object>> list = service.searchPos(keyword, pageNo, pageSize);
        return list;
    }

    /**
     * 导入数据
     * @return
     */
    @RequestMapping("/importAll")
    @ResponseBody
    public String importAll() {
        try {
            service.importAll();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "success";
    }
}
