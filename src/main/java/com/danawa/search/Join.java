package com.danawa.search;

import org.elasticsearch.search.SearchHit;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Join implements Serializable {
    private String index;
    private String parent;
    private String child;
    private List<Map<String, Object>> should;
    private int minimumShouldMatch;

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

        if (joinMap.get("should") != null) {
            Object shouldObject = joinMap.get("should");
            if (shouldObject instanceof ArrayList) {
                this.should = (ArrayList) shouldObject;
            } else if (shouldObject instanceof HashMap) {
                this.should = new ArrayList<>();
                this.should.add((Map <String, Object>)shouldObject);
            } else {
                this.should = new ArrayList<>();
            }
        } else {
            this.should = new ArrayList<>();
        }

        if (joinMap.get("minimum_should_match") != null) {
            this.minimumShouldMatch = (int) joinMap.get("minimum_should_match");
        } else {
            this.minimumShouldMatch = 0;
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

    public List<Map<String, Object>> getShould() {
        return should;
    }

    public void setShould(List<Map<String, Object>> should) {
        this.should = should;
    }

    public int getMinimumShouldMatch() {
        return minimumShouldMatch;
    }

    public void setMinimumShouldMatch(int minimumShouldMatch) {
        this.minimumShouldMatch = minimumShouldMatch;
    }
}
