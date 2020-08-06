package com.danawa.search;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.queryparser.flexible.core.builders.QueryBuilder;
import org.apache.lucene.queryparser.xml.QueryBuilderFactory;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.json.JsonXContentGenerator;
import org.elasticsearch.common.xcontent.json.JsonXContentParser;
import org.elasticsearch.common.xcontent.support.MapXContentParser;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.PluginInfo;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.RestActionListener;
import org.elasticsearch.rest.action.RestResponseListener;
import org.elasticsearch.rest.action.cat.RestTable;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONWriter;

import java.io.IOException;
import java.io.StringWriter;
import java.util.*;

import static org.elasticsearch.rest.RestStatus.OK;

public class LeftJoinAction extends BaseRestHandler {
    private static Logger logger = Loggers.getLogger(LeftJoinAction.class, "");

    private static final String CONTENT_TYPE_JSON = "application/json;charset=UTF-8";

    @Inject
    public LeftJoinAction(Settings settings, RestController controller) {
        controller.registerHandler(RestRequest.Method.GET, "/{index}/_left-join", this);
        controller.registerHandler(RestRequest.Method.POST, "/{index}/_left-join", this);
    }

    @Override
    public String getName() {
        return "rest_handler_left_join_plugin";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        JSONObject content = JSONUtils.parseRequestBody(request);
        String index = request.param("index", "");
        String action = request.param("action", "");
        logger.debug("index: {}, action: {}", index, action);

        JSONObject query  = content.getJSONObject("query");
        JSONObject join   = content.getJSONObject("join");

        String parentField = join.getString("parent-field");
        String childIndex = join.getString("index");
        String childField = join.getString("child-field");

        SearchResponse parentResponse = search(client, index, query.toString());
        SearchHit[] parentSearchHits = parentResponse.getHits().getHits();

        Set<String> joinKeys = new LinkedHashSet<>();
        for (int i = 0; i < parentSearchHits.length; i++) {
            Map<String, String> source = JSONUtils.flattenToStringMap(parentSearchHits[i].getSourceAsMap());
            joinKeys.add(source.get(parentField));
        }

        XContentBuilder childContentBuilder = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("bool")
                        .startObject("should")
                            .startObject("terms")
                                .field(childField, joinKeys)
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();
        List<SearchResponse> childResponseList = searchAll(client, childIndex, Strings.toString(childContentBuilder));


        for (int i = 0; i < parentSearchHits.length; i++) {
            Map<String, String> parentSource = JSONUtils.flattenToStringMap(parentSearchHits[i].getSourceAsMap());
            String parentJoinKey = parentSource.get(parentField);
            List<SearchHit> tmpChildSearchHits = new ArrayList<>();
            float maxScore = 0.0f;

            if (parentJoinKey != null && parentJoinKey.trim().length() > 0) {
                for (int j = 0; j < childResponseList.size(); j++) {
                    SearchResponse childSearchResponse = childResponseList.get(j);
                    SearchHit[] childSearchHits = childSearchResponse.getHits().getHits();

                    for (int k = 0; k < childSearchHits.length; k++) {
                        Map<String, String> childSource = JSONUtils.flattenToStringMap(childSearchHits[k].getSourceAsMap());
                        String childJoinKey = childSource.get(childField);
                        if (parentJoinKey.equals(childJoinKey)) {
                            tmpChildSearchHits.add(childSearchHits[k]);
                            if (childSearchHits[k].getScore() > maxScore) {
                                maxScore = childSearchHits[k].getScore();
                            }
                        }
                    }
                }
            }

            SearchHit[] tmp = tmpChildSearchHits.toArray(new SearchHit[0]);
            Map<String, SearchHits> child = new HashMap<>();
            child.put("child", new SearchHits(tmp, new TotalHits(tmpChildSearchHits.size(), TotalHits.Relation.EQUAL_TO), maxScore));
            parentSearchHits[i].setInnerHits(child);
        }

//        XContentType.fromMediaType(request.getXContentType().mediaType()).
        XContentBuilder.builder(XContentType.fromMediaType(request.getXContentType().mediaType()).xContent());
//        request.getXContentType()
        XContentBuilder parentBuilder = parentResponse.toXContent(JsonXContent.contentBuilder().prettyPrint(), ToXContent.EMPTY_PARAMS).prettyPrint();
//        final XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
//        Map<String, Object> response = new HashMap<>();
//        response.put("docs", docs);
//        builder.map(response);
        return channel -> channel.sendResponse(new BytesRestResponse(OK, parentBuilder));
    }









    static SearchResponse search(NodeClient client, String index, String query) {
        SearchResponse searchResponse = null;
        try {
            SearchSourceBuilder source = new SearchSourceBuilder();
            source.query(QueryBuilders.wrapperQuery(query));
            SearchRequest search = new SearchRequest(index.split("[,]"));
            search.source(source);
            searchResponse = client.search(search).actionGet();
        } catch (Exception e) {
            logger.error("", e);
        }
        return searchResponse;
    }
    static List<SearchResponse> searchAll(NodeClient client, String index, String query) {
        Scroll scroll = null;
        String scrollId = null;
        List<SearchResponse> responseList = new ArrayList<>();
        try {
            SearchSourceBuilder source = new SearchSourceBuilder();
            source.query(QueryBuilders.wrapperQuery(query));
            SearchRequest search = new SearchRequest(index.split("[,]"));
            scroll = new Scroll(TimeValue.MINUS_ONE);
            search.source(source);
            search.scroll(scroll);
            SearchResponse response = client.search(search).actionGet();
            responseList.add(response);
            scrollId = response.getScrollId();

            while (true) {
                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(scroll);
                response = client.searchScroll(scrollRequest).actionGet();
                if (response.getHits().getHits().length == 0) {
                    break;
                }
                responseList.add(response);
                scrollId = response.getScrollId();
            }
        } catch (Exception e) {
            logger.error("", e);
        }
        return responseList;
    }


}
