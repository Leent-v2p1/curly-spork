package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.replica_on_date;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.base.WorkflowType;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.DatamartProperties;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.WfResourcesGenerator;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.WorkflowWithResources;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans.Action;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.properties.PropertiesBuilderMapper;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.replica_on_date.configuration.ReplicaConf;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.replica_on_date.configuration.ReplicaExtractor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class WorkflowForReplicaCreator {
    private static final int MAX_SIZE_OF_FORK = 10;
    private final ForkCreator forkCreator;
    private final ReplicaExtractor replicaExtractor;
    private final Environment environment;

    public WorkflowForReplicaCreator(ForkCreator forkCreator, ReplicaExtractor replicaExtractor, Environment environment) {
        this.forkCreator = forkCreator;
        this.replicaExtractor = replicaExtractor;
        this.environment = environment;
    }

    public WorkflowWithResources create(DatamartProperties datamartConfig) {
        List<String> sourceReplicaTables = datamartConfig.getSourceReplicaTables();
        FullTableName datamart = FullTableName.of(datamartConfig.getTargetSchema(), datamartConfig.getTargetTable());

        Map<String, ReplicaConf> replicaActions = replicaExtractor.getReplicaTableConfigurations(sourceReplicaTables, datamart.dbName());
        PropertiesBuilderMapper replicaPropertiesBuilderMapper = new ReplicaOnDatePropertiesBuilderMapper(replicaActions, environment, datamart);
        WfResourcesGenerator wfShellGenerator = new WfResourcesGenerator(replicaActions, replicaPropertiesBuilderMapper,
                environment, replicaActions);
        List<String> replicaTables = getReplicaTables(replicaActions);
        List<Action> forkWithMaxSize = forkCreator.createForkWithMaxSize(new ArrayList<>(replicaTables), MAX_SIZE_OF_FORK);
        return wfShellGenerator.generate(forkWithMaxSize, "replica_for_" + datamart.fullTableName());
    }

    private List<String> getReplicaTables(Map<String, ReplicaConf> replicaActions) {
        return replicaActions.entrySet().stream()
                .filter(entry -> WorkflowType.DATAMART.equals(WorkflowType.valueOfByKey(entry.getValue().getType())))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }
}
