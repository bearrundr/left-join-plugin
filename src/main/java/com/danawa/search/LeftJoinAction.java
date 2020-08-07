package com.danawa.search;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.rest.RestStatus.OK;

public class LeftJoinAction extends BaseRestHandler {
    private static Logger logger = Loggers.getLogger(LeftJoinAction.class, "");

    private static final String JOIN_FIELD = "join";


    @Inject
    public LeftJoinAction(Settings settings, RestController controller) {
        controller.registerHandler(RestRequest.Method.GET, "/{index}/_left", this);
        controller.registerHandler(RestRequest.Method.POST, "/{index}/_left", this);
    }

    @Override
    public String getName() {
        return "rest_handler_left_join_plugin";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        try {
            JSONObject content = JSONUtils.parseRequestBody(request);
            String parentIndices = request.param("index");

//            1. 조인 필드 추출
            JSONArray joinArr = new JSONArray();
            if (content.has(JOIN_FIELD)) {
                try {
                    joinArr = content.getJSONArray(JOIN_FIELD);
                } catch (Exception e) {
                    JSONObject joinJsonObject = content.getJSONObject(JOIN_FIELD);
                    joinArr.put(joinJsonObject);
                }
                content.remove(JOIN_FIELD);
            }

            if (joinArr.length() == 0) {
                throw new IOException("join Arrays Empty");
            }

//            2. 메인 쿼리 조회
            SearchResponse parentResponse = EsUtils.search(request, client, parentIndices, content.toString());
            SearchHits parentSearchHits = parentResponse.getHits();

//            3. 메인 쿼리 연관 키워드 조인 검색
            List<Join> joins = new ArrayList<>();
            List<Object> objectJoinList = joinArr.toList();
            int objectJoinListSize = objectJoinList.size();
            for (int i = 0; i < objectJoinListSize; i++) {
                Join join = new Join((Map<String, Object>)objectJoinList.get(i));
                Set<String> relationalValues = extractValues(parentSearchHits, join.getParent());
                List<SearchHit> childSearchHits = EsUtils.childSearch(client, join, relationalValues);
                join.setSearchHits(childSearchHits);
                joins.add(join);
            }

//            4. parent innerHit 에 child hit 추가
            SearchHit[] parentSearchHitArr = parentSearchHits.getHits();
            for (int i = 0; i < parentSearchHitArr.length; i++) {
                SearchHit searchHit = parentSearchHitArr[i];
                Map<String, String> parentFlatMap = JSONUtils.flattenToStringMap(searchHit.getSourceAsMap());

                float maxScore = 0.0f;
                List<SearchHit> tmpChildSearchHits = new ArrayList<>();
                int joinsSize = joins.size();
                for (int j = 0; j < joinsSize; j++) {
                    Join join = joins.get(j);
                    String parent = parentFlatMap.get(join.getParent());
                    if (parent != null) {
                        List<SearchHit> childSearchHits = join.getSearchHits();
                        int childSearchHitsSize = childSearchHits.size();
                        for (int k = 0; k < childSearchHitsSize; k++) {
                            SearchHit childSearchHit = childSearchHits.get(k);
                            Map<String, String> childFlatMap = JSONUtils.flattenToStringMap(childSearchHit.getSourceAsMap());
                            String child = childFlatMap.get(join.getChild());
                            if (parent.equals(child)) {
                                tmpChildSearchHits.addAll(childSearchHits);
                                if (maxScore < join.getMaxScore()) {
                                    maxScore = join.getMaxScore();
                                }
                                break;
                            }
                        }
                    }
                }

                // append child
                Map<String, SearchHits> child = new HashMap<>();
                child.put("_child",
                        new SearchHits(tmpChildSearchHits.toArray(new SearchHit[0]),
                        new TotalHits(tmpChildSearchHits.size(), TotalHits.Relation.EQUAL_TO),
                        maxScore));
                parentSearchHitArr[i].setInnerHits(child);
            }

            return channel -> {
                XContentBuilder xContentBuilder = channel.newBuilder(XContentType.JSON, true);
                parentResponse.toXContent(xContentBuilder, new ToXContent.MapParams(request.params()));
                BytesRestResponse bytesRestResponse = new BytesRestResponse(OK, xContentBuilder);
                channel.sendResponse(bytesRestResponse);
            };
        } catch (Throwable e) {
            logger.error("[LEFT JOIN PLUGIN ERROR]", e);
            throw new IOException("[LEFT JOIN PLUGIN ERROR] " + e.getMessage(), e);
        }
    }


    Set<String> extractValues(SearchHits searchHits, String field) {
        if (searchHits.getTotalHits().value == 0) {
            return new HashSet<>();
        }

        Set<String> extractValues = new HashSet<>();
        Iterator<SearchHit> iterator = searchHits.iterator();
        while (iterator.hasNext()) {
            SearchHit searchHit = iterator.next();
            Map<String, String> flatSourceMap = JSONUtils.flattenToStringMap(searchHit.getSourceAsMap());
            String val = flatSourceMap.get(field);
            if (val != null) {
                extractValues.add(val);
            }
        }
        return extractValues;
    }

}
