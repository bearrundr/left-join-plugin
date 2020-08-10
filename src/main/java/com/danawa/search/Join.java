package com.danawa.search;

import org.elasticsearch.search.SearchHit;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Join implements Serializable {
    private String index;
    private String parent;
    private String child;
    private List<Map<String, Object>> query;


    private float maxScore;
    private List<SearchHit> searchHits;

    private Join() { }


    public Join(Map<String, Object> joinMap) {
        if (joinMap.get("index") != null) {
            this.setIndex((String) joinMap.get("index"));
        }
        if (joinMap.get("parent") != null) {
            this.setParent((String) joinMap.get("parent"));
        }
        if (joinMap.get("child") != null) {
            this.setChild((String) joinMap.get("child"));
        }
        if (joinMap.get("query") != null) {
            Object queryObject = joinMap.get("query");
            if (queryObject instanceof ArrayList) {
                this.query = (List<Map<String, Object>>) queryObject;
            } else {
                this.query = new ArrayList<>();
                this.query.add((Map<String, Object>) queryObject);
            }
        } else {
            this.query = new ArrayList<>();
        }
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getParent() {
        return parent;
    }

    public void setParent(String parent) {
        this.parent = parent;
    }

    public String getChild() {
        return child;
    }

    public void setChild(String child) {
        this.child = child;
    }

    public float getMaxScore() {
        return maxScore;
    }

    public void setMaxScore(float maxScore) {
        this.maxScore = maxScore;
    }

    public List<SearchHit> getSearchHits() {
        return searchHits;
    }

    public void setSearchHits(List<SearchHit> searchHits) {
        this.searchHits = searchHits;
    }

    public List<Map<String, Object>> getQuery() {
        return query;
    }

    public void setQuery(List<Map<String, Object>> query) {
        this.query = query;
    }
}
