package com.danawa.search.plugins;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;

import java.util.List;
import java.util.function.Supplier;


public class LeftJoinPlugin extends Plugin implements ActionPlugin {

    private static Logger logger = Loggers.getLogger(LeftJoinPlugin.class, "");

    public LeftJoinPlugin() {
        logger.trace("init");
    }

    /**
     * 액션 핸들러 등록
     * ES 에 상호작용이 가능한 REST 액션 핸들러를 등록한다.
     */
    @Override public List<RestHandler> getRestHandlers(Settings settings, RestController controller,
                                                       ClusterSettings clusterSettings, IndexScopedSettings indexScopedSettings, SettingsFilter filter,
                                                       IndexNameExpressionResolver resolver, Supplier<DiscoveryNodes> nodes) {
//        return singletonList(new ProductNameAnalysisAction(settings, controller));
        return null;
    }
}
