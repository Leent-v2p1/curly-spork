package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.properties.builder;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.DatamartProperties;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.ReservingProperties;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.MemoryParams;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.ActionConf;

import java.util.Map;

public class SparkReservingPropertiesBuilder extends SparkPropertiesBuilder<ActionConf> {

    private ReservingProperties reservingProperties;

    public SparkReservingPropertiesBuilder(String actionName,
                                           ActionConf actionConf,
                                           Environment env,
                                           DatamartProperties properties) {
        super(actionName, actionConf, env, properties);
        reservingProperties = properties.reservingProperties();
    }

    @Override
    protected Map<String, Object> getParams() {
        Map<String, Object> params = super.getParams();
        params.put("resultSchema", reservingProperties.getTargetSchema());
        params.put("resultTable", reservingProperties.getTargetTable());
        final Map<String, String> customJavaOpts = properties.customJavaOpts();
        final Map<String, String> reservingCustomJavaOpts = reservingProperties.customJavaOpts();
        customJavaOpts.putAll(reservingCustomJavaOpts);
        params.put("customJavaOpts", addSparkPrefix(customJavaOpts).entrySet());
        return params;
    }

    @Override
    protected MemoryParams getMemoryParams() {
        actionConf.setDriverMemory(reservingProperties.getDriverMemory());
        return MemoryParams.memoryParamsBasedOnActionConf(actionConf, MemoryParams.minimalMemoryParams());
    }
}
