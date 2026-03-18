package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.replica_on_date;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.YamlPropertiesParser;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.file.TargetPathBuilder;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.WorkflowWithResources;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.write.FileWriter;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.write.WorkflowWriter;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.replica_on_date.configuration.HistoricalDatamartsExtractor;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.replica_on_date.configuration.ReplicaActionDependency;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.replica_on_date.configuration.ReplicaConf;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.replica_on_date.configuration.ReplicaExtractor;

import java.nio.file.Path;
import java.util.Map;

import static java.util.stream.Collectors.toMap;

/**
 * генерирует workflow для восстановления реплик на день, полученные из datamart-properties.yaml и replica-properties.yaml
 * записывает сгенерированная структуру потоков на диск
 */
public class ReplicaOnDateGenerator {

    private final Environment env;
    private WorkflowWriter workflowWriter;
    private FileWriter fileWriter;
    private ReplicaExtractor replicaExtractor;
    private HistoricalDatamartsExtractor historicalDatamartsExtractor;
    private WorkflowForReplicaCreator workflowCreator;
    private TargetPathBuilder pathBuilder;

    public ReplicaOnDateGenerator(Environment env) {
        this.env = env;
    }

    public ReplicaOnDateGenerator(Environment env,
                                  WorkflowWriter workflowWriter,
                                  FileWriter fileWriter,
                                  ReplicaExtractor replicaExtractor,
                                  HistoricalDatamartsExtractor historicalDatamartsExtractor,
                                  WorkflowForReplicaCreator workflowCreator,
                                  TargetPathBuilder pathBuilder) {
        this.env = env;
        this.workflowWriter = workflowWriter;
        this.fileWriter = fileWriter;
        this.replicaExtractor = replicaExtractor;
        this.historicalDatamartsExtractor = historicalDatamartsExtractor;
        this.workflowCreator = workflowCreator;
        this.pathBuilder = pathBuilder;
    }

    public void init() {
        final Map<String, Object> replicaProperties = new YamlPropertiesParser().parse("/replica-properties.yaml");
        final ReplicaActionDependency replicaActionDependency = new ReplicaActionDependency(replicaProperties);
        final Map<String, ReplicaConf> replicaConfMap = replicaActionDependency.resolve();
        this.workflowWriter = new WorkflowWriter();
        this.fileWriter = new FileWriter();
        this.replicaExtractor = new ReplicaExtractor(replicaConfMap);
        this.workflowCreator = new WorkflowForReplicaCreator(new ForkCreator(), replicaExtractor, env);
        this.historicalDatamartsExtractor = new HistoricalDatamartsExtractor();
        this.pathBuilder = new TargetPathBuilder(env);
    }

    public void generateAndWriteWorkflows() {
        writeWorkflows(generateWorkflows());
    }

    protected Map<FullTableName, WorkflowWithResources> generateWorkflows() {
        return historicalDatamartsExtractor
                .getHistoricalDatamarts()
                .entrySet()
                .stream()
                .collect(toMap(Map.Entry::getKey,
                        namePropertiesServiceEntry -> workflowCreator.create(namePropertiesServiceEntry.getValue())));
    }

    protected void writeWorkflows(Map<FullTableName, WorkflowWithResources> workflows) {
        workflows.forEach((datamart, workflowWithShell) -> {
                    Path pathToWorkflow = pathBuilder.resolveReplicaOnDatePath(datamart);
                    workflowWriter.writeWorkflow(pathToWorkflow, workflowWithShell.workflow);
                    fileWriter.write(pathToWorkflow, workflowWithShell.fileContentList);
                }
        );
    }
}
