package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.properties.builder;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.DatamartProperties;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.MemoryParams;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.ActionConf;

public class CtlStatisticPublisherPropertiesBuilder extends SparkPropertiesBuilder<ActionConf> {

    public CtlStatisticPublisherPropertiesBuilder(String actionName, ActionConf actionConf, Environment env, DatamartProperties properties) {
        super(actionName, actionConf, env, properties);
    }

    @Override
    protected MemoryParams getMemoryParams() {
        return MemoryParams.minimalMemoryParams();
    }
}
