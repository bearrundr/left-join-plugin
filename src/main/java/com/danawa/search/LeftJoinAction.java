package com.danawa.search;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.json.JSONObject;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.rest.RestStatus.OK;

public class LeftJoinAction extends BaseRestHandler {
    private static Logger logger = Loggers.getLogger(LeftJoinAction.class, "");


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
        try {
            JSONObject content = JSONUtils.parseRequestBody(request);
            String parentIndices = request.param("index", "");
            String explain = request.params().getOrDefault("explain", "false");
            Settings.Builder settingsBuilder = Settings.builder();
            for (Map.Entry<String, String> param : request.params().entrySet()) {
                settingsBuilder.put(param.getKey(), param.getValue());

            }
            Settings settings = settingsBuilder.build();

            JSONObject join = content.getJSONObject("join");
            content.remove("join");

            String parentField = join.getString("parent-field");

            String childIndices = join.getString("indices");
            String childField = join.getString("child-field");

            SearchResponse parentResponse = search(client, parentIndices, content.toString(), settings);
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
            List<SearchResponse> childResponseList = searchAll(client, childIndices, Strings.toString(childContentBuilder));

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

            return channel -> {
                XContentBuilder xContentBuilder = channel.newBuilder(XContentType.JSON, true);
                parentResponse.toXContent(xContentBuilder, new ToXContent.MapParams(request.params()));
                BytesRestResponse bytesRestResponse = new BytesRestResponse(OK, xContentBuilder);
                channel.sendResponse(bytesRestResponse);
            };
        } catch (Throwable e) {
            logger.error("[JOIN PLUGIN ERROR]", e);
            throw new IOException("[JOIN PLUGIN ERROR] " + e.getMessage(), e);
        }
    }






    static SearchResponse search(NodeClient client, String indices, String query, Settings settings) throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        SearchModule searchModule = new SearchModule(settings, false, Collections.emptyList());
        try (XContentParser parser = XContentFactory
                .xContent(XContentType.JSON)
                .createParser(new NamedXContentRegistry(searchModule.getNamedXContents()),
                        DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                        query)) {
            searchSourceBuilder.parseXContent(parser);
        }

        SearchRequest search = new SearchRequest(indices.split("[,]"), searchSourceBuilder);
        SearchResponse searchResponse = client.search(search).get();
        return searchResponse;
    }
    static List<SearchResponse> searchAll(NodeClient client, String indices, String query) {
        Scroll scroll;
        String scrollId;
        List<SearchResponse> responseList = new ArrayList<>();
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        try {
            SearchSourceBuilder source = new SearchSourceBuilder();
            source.query(QueryBuilders.wrapperQuery(query));
            SearchRequest search = new SearchRequest(indices.split("[,]"));
            scroll = new Scroll(TimeValue.MINUS_ONE);
            search.source(source);
            search.scroll(scroll);
            SearchResponse response = client.search(search).actionGet();
            responseList.add(response);
            scrollId = response.getScrollId();
            clearScrollRequest.addScrollId(scrollId);
            while (true) {
                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(scroll);
                response = client.searchScroll(scrollRequest).actionGet();
                if (response.getHits().getHits().length == 0) {
                    break;
                }
                responseList.add(response);
                scrollId = response.getScrollId();
                clearScrollRequest.addScrollId(scrollId);
            }
            client.clearScroll(clearScrollRequest);
        } catch (Exception e) {
            logger.error("", e);
        }
        return responseList;
    }


}
