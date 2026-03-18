package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.properties.builder;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.DatamartProperties;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.ActionConf;

import java.util.Map;

public class SparkStagePropertiesBuilder extends SparkPropertiesBuilder<ActionConf> {

    public SparkStagePropertiesBuilder(String actionName,
                                       ActionConf actionConf,
                                       Environment env,
                                       DatamartProperties properties) {
        super(actionName, actionConf, env, properties);
    }

    @Override
    protected Map<String, Object> getParams() {
        Map<String, Object> params = super.getParams();
        params.put("resultSchema", properties.getTargetSchema());
        params.put("resultTable", properties.getTargetTable());
        return params;
    }
}
