package com.danawa.search;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

public class JSONUtils {

    static JSONObject parseRequestBody(RestRequest request) {
        JSONObject ret = new JSONObject();
        try {
            ret = new JSONObject(new JSONTokener(request.content().utf8ToString()));
        } catch (Exception ignore) { }
        return ret;
    }


    static Map<String, String> flattenToStringMap (
            Map<String, ? extends Object> inputMap) {

        Map<String, String> resultMap = new LinkedHashMap<>();

        doFlatten("", inputMap.entrySet().iterator(), resultMap,
                it -> it == null ? null : it.toString());

        return resultMap;
    }

    private static void doFlatten(String propertyPrefix,
                                  Iterator<? extends Map.Entry<String, ?>> inputMap,
                                  Map<String, ? extends Object> resultMap,
                                  Function<Object, Object> valueTransformer) {

        if (propertyPrefix != null && !"".equals(propertyPrefix)) {
            propertyPrefix = propertyPrefix + ".";
        }

        while (inputMap.hasNext()) {

            Map.Entry<String, ? extends Object> entry = inputMap.next();
            flattenElement(propertyPrefix.concat(entry.getKey()), entry.getValue(),
                    resultMap, valueTransformer);
        }
    }

    private static void flattenElement(String propertyPrefix, Object source,
                                       Map<String, ?> resultMap, Function<Object, Object> valueTransformer) {

        if (source instanceof Iterable) {
            flattenCollection(propertyPrefix, (Iterable<Object>) source, resultMap,
                    valueTransformer);
            return;
        }

        if (source instanceof Map) {
            doFlatten(propertyPrefix, ((Map<String, ?>) source).entrySet().iterator(),
                    resultMap, valueTransformer);
            return;
        }

        ((Map) resultMap).put(propertyPrefix, valueTransformer.apply(source));
    }

    private static void flattenCollection(String propertyPrefix,
                                          Iterable<Object> iterable, Map<String, ?> resultMap,
                                          Function<Object, Object> valueTransformer) {

        int counter = 0;

        for (Object element : iterable) {
            flattenElement(propertyPrefix + "[" + counter + "]", element, resultMap,
                    valueTransformer);
            counter++;
        }
    }

}
