package com.danawa.search;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.*;

public class EsUtils {
    private static Logger logger = Loggers.getLogger(EsUtils.class, "");

    static SearchResponse search(RestRequest request, NodeClient client, String indices, String query) throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        SearchModule searchModule = new SearchModule(getSettings(request), false, Collections.emptyList());
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
    static List<SearchHit> childSearch(NodeClient client, Join join, Set<String> relationalValues) throws IOException {
        List<SearchHit> searchHitList = new ArrayList<>();
        if (relationalValues.size() == 0) {
            return searchHitList;
        }
        Scroll scroll;
        String scrollId;
        SearchResponse response;
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();

        try {
            List<Map<String, Object>> mustList = new ArrayList<>();
            mustList.add(Map.of("terms", Map.of(join.getChild(), relationalValues)));
            mustList.addAll(join.getQuery() != null ? join.getQuery() : new ArrayList<>());

            XContentBuilder childContentBuilder = XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("bool")
                    .field("must").value(mustList)
                    .endObject()
                    .endObject();

            SearchSourceBuilder source = new SearchSourceBuilder();
            source.query(QueryBuilders.wrapperQuery(Strings.toString(childContentBuilder)));
            SearchRequest search = new SearchRequest(join.getIndex().split("[,]"));
            scroll = new Scroll(TimeValue.MINUS_ONE);
            search.source(source);
            search.scroll(scroll);
            response = client.search(search).actionGet();

            while (response.getHits().getHits().length != 0) {
                scrollId = response.getScrollId();
                clearScrollRequest.addScrollId(scrollId);
                searchHitList.addAll(Arrays.asList(response.getHits().getHits()));
                if (join.getMaxScore() < response.getHits().getMaxScore()) {
                    join.setMaxScore(response.getHits().getMaxScore());
                }
                response = client.searchScroll(new SearchScrollRequest(scrollId).scroll(scroll)).actionGet();
            }
        } catch (Exception e) {
            logger.error("", e);
            throw new IOException(e);
        } finally {
            client.clearScroll(clearScrollRequest);
        }
        return searchHitList;
    }

    static Settings getSettings(RestRequest request) {
        Settings.Builder settingsBuilder = Settings.builder();
        for (Map.Entry<String, String> param : request.params().entrySet()) {
            settingsBuilder.put(param.getKey(), param.getValue());
        }
        return settingsBuilder.build();
    }

}
