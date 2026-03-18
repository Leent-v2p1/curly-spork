package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.properties.builder;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.DatamartProperties;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.MemoryParams;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.ActionConf;

import java.util.Map;

public class SparkRepartitionerPropertiesBuilder extends SparkPropertiesBuilder<ActionConf> {

    private final ActionConf datamartActionConf;

    public SparkRepartitionerPropertiesBuilder(String actionName,
                                               ActionConf actionConf,
                                               ActionConf datamartActionConf,
                                               Environment env,
                                               DatamartProperties properties) {
        super(actionName, actionConf, env, properties);
        this.datamartActionConf = datamartActionConf;
    }

    @Override
    protected Map<String, Object> getParams() {
        final Map<String, Object> params = super.getParams();
        params.put("repartitionEnabled", true);
        return params;
    }

    @Override
    protected MemoryParams getMemoryParams() {
        return MemoryParams.memoryParamsForRepartitioner(datamartActionConf, MemoryParams.defaultMemoryParams());
    }
}
