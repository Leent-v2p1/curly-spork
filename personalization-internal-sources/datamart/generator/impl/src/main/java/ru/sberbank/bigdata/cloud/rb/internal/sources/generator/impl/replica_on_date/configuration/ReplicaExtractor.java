package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.replica_on_date.configuration;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.NameAdditions;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.ActionConf;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.NameAdditions.STG_POSTFIX;

public class ReplicaExtractor {
    private final Map<String, ReplicaConf> replicaConfig;

    public ReplicaExtractor(Map<String, ReplicaConf> read) {
        this.replicaConfig = read;
    }

    public Map<String, ReplicaConf> getReplicaTableConfigurations(List<String> replicas, String sourceSchema) {
        Map<String, ReplicaConf> result = new HashMap<>();
        for (String replicaTable : replicas) {
            String replica = sourceSchema + STG_POSTFIX + "." + replicaTable;
            String replicaProperty = replica + NameAdditions.PROPERTIES_POSTFIX;
            ReplicaConf replicaConf = replicaConfig.get(replica);
            ReplicaConf replicaPropertyConf = replicaConfig.get(replicaProperty);
            checkConfExists(replicaConf, replica);
            checkConfExists(replicaPropertyConf, replicaProperty);
            result.put(replica, replicaConf);
            result.put(replicaProperty, replicaPropertyConf);
        }
        return result;
    }

    private void checkConfExists(ActionConf conf, String name) {
        if (conf == null) {
            throw new IllegalStateException(String.format("Couldn't find in replica config entity with name: '%s'", name));
        }
    }
}
