package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.replica_on_date;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.ActionConf;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans.Action;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.properties.PropertiesBuilderMapper;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.properties.builder.SparkPropertiesBuilder;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.replica_on_date.configuration.ReplicaConf;

import java.util.Map;

public class ReplicaOnDatePropertiesBuilderMapper implements PropertiesBuilderMapper {

    private final Map<String, ReplicaConf> replicaConfigs;
    private final Environment environment;
    private final FullTableName datamartName;

    public ReplicaOnDatePropertiesBuilderMapper(Map<String, ReplicaConf> replicaConfigs,
                                                Environment environment,
                                                FullTableName datamartName) {
        this.replicaConfigs = replicaConfigs;
        this.environment = environment;
        this.datamartName = datamartName;
    }

    @Override
    public SparkPropertiesBuilder<? extends ActionConf> getPropertiesBuilder(Action action) {
        final String name = action.name();
        return new ReplicaOnDatePropertiesBuilder(name, replicaConfigs.get(name), environment, datamartName);
    }
}
