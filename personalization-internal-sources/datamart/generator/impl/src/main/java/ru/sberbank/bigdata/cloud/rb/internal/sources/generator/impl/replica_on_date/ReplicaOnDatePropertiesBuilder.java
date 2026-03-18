package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.replica_on_date;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.properties.builder.SparkPropertiesBuilder;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.replica_on_date.configuration.ReplicaConf;

import java.util.HashMap;
import java.util.Map;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.CommonFiles.PERSONALIZATION_JAR;

public class ReplicaOnDatePropertiesBuilder extends SparkPropertiesBuilder<ReplicaConf> {

    private final FullTableName datamartName;

    public ReplicaOnDatePropertiesBuilder(String actionName,
                                          ReplicaConf actionConf,
                                          Environment env,
                                          FullTableName datamartName) {
        super(actionName, actionConf, env, null);
        this.datamartName = datamartName;
    }

    @Override
    protected Map<String, Object> getParams() {
        FullTableName replicaTableName = FullTableName.of(actionName);

        Map<String, Object> params = new HashMap<>();

        params.put("jar", PERSONALIZATION_JAR);
        params.put("className", actionConf.getClassName());
        params.put("resultSchema", replicaTableName.dbName());
        params.put("resultTable", replicaTableName.tableName());
        params.put("datamartTableName", datamartName);
        params.put("historyTableName", actionName);
        params.put("recoveryDate", "\'${recoveryDate}\'");
        return params;
    }
}
