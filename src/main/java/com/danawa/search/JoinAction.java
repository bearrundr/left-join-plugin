//package com.danawa.search;
//
//import org.apache.logging.log4j.Logger;
//import org.apache.lucene.search.TotalHits;
//import org.elasticsearch.action.search.*;
//import org.elasticsearch.client.node.NodeClient;
//import org.elasticsearch.common.Strings;
//import org.elasticsearch.common.inject.Inject;
//import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
//import org.elasticsearch.common.io.stream.StreamInput;
//import org.elasticsearch.common.logging.Loggers;
//import org.elasticsearch.common.settings.Settings;
//import org.elasticsearch.common.unit.TimeValue;
//import org.elasticsearch.common.xcontent.*;
//import org.elasticsearch.index.query.QueryBuilders;
//import org.elasticsearch.rest.BaseRestHandler;
//import org.elasticsearch.rest.BytesRestResponse;
//import org.elasticsearch.rest.RestController;
//import org.elasticsearch.rest.RestRequest;
//import org.elasticsearch.search.Scroll;
//import org.elasticsearch.search.SearchHit;
//import org.elasticsearch.search.SearchHits;
//import org.elasticsearch.search.SearchModule;
//import org.elasticsearch.search.builder.SearchSourceBuilder;
//import org.elasticsearch.search.profile.SearchProfileShardResults;
//import org.json.JSONArray;
//import org.json.JSONObject;
//
//import java.io.IOException;
//import java.util.*;
//
//import static org.elasticsearch.rest.RestStatus.OK;
//
//public class JoinAction extends BaseRestHandler {
//    private static Logger logger = Loggers.getLogger(JoinAction.class, "");
//
//
//    private static final String JOIN_FIELD = "join";
//
//
//    @Inject
//    public JoinAction(Settings settings, RestController controller) {
//        controller.registerHandler(RestRequest.Method.GET, "/{index}/_join", this);
//        controller.registerHandler(RestRequest.Method.POST, "/{index}/_join", this);
//    }
//
//    @Override
//    public String getName() {
//        return "rest_handler_left_join_plugin";
//    }
//
//    @Override
//    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
//        try {
//            JSONObject content = JSONUtils.parseRequestBody(request);
//            String parentIndices = request.param("index");
//
////            1. 조인 필드 추출
//            JSONArray joinArr = new JSONArray();
//            if (content.has(JOIN_FIELD)) {
//                joinArr = content.getJSONArray(JOIN_FIELD);
//                content.remove(JOIN_FIELD);
//            }
//
////            2. 메인 쿼리 조회
//            SearchResponse parentResponse = EsUtils.search(request, client, parentIndices, content.toString());
//            SearchHits parentSearchHits = parentResponse.getHits();
//
////            3. 조인 로직
//            Iterator<Object> iterator = joinArr.iterator();
//            while (iterator.hasNext()) {
//                Join join = new Join(iterator.next());
//
//                logger.debug("{}", join);
//            }
//
//
//            return channel -> {
//                XContentBuilder xContentBuilder = channel.newBuilder(XContentType.JSON, true);
//                parentResponse.toXContent(xContentBuilder, new ToXContent.MapParams(request.params()));
//                BytesRestResponse bytesRestResponse = new BytesRestResponse(OK, xContentBuilder);
//                channel.sendResponse(bytesRestResponse);
//            };
//        } catch (Throwable e) {
//            logger.error("[LEFT JOIN PLUGIN ERROR]", e);
//            throw new IOException("[LEFT JOIN PLUGIN ERROR] " + e.getMessage(), e);
//        }
//    }
//
//}
