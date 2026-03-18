package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.prereqs;

import org.yaml.snakeyaml.Yaml;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.DatamartProperties;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.YamlPropertiesParser;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.prereqs.entites.*;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

/**
 * Reads prerequisites config workflow-dependencies.yaml and map it to DependencyConf
 */
public class PrereqsReader {

    private static final String AND_EVENT_AWAIT_SREATEGY = "and";
    PrereqConf[] prereqConfs;
    private Map<String, Object> customProperties;

    public PrereqsReader() {
        final Yaml yaml = new Yaml();
        this.prereqConfs = yaml.loadAs(this.getClass().getResourceAsStream("/workflow-dependencies.yaml"), PrereqConf[].class);
    }

    PrereqsReader(String confPath, String datamartPropertiesPath) {
        final Yaml yaml = new Yaml();
        this.prereqConfs = yaml.loadAs(this.getClass().getResourceAsStream(confPath), PrereqConf[].class);
        this.customProperties = new YamlPropertiesParser().parse(datamartPropertiesPath);
    }

    public DependencyConf prereqConf(String name) {
        final Optional<PrereqConf> conf = findConfig(name);
        WorkflowParameters defaultParameters = new WorkflowParameters();
        if (!conf.isPresent()) {
            return new DependencyConf(defaultParameters);
        }

        PrereqConf prereqConf = conf.get();
        Dependencies dependencies = prereqConf.getDependencies();
        WorkflowParameters parameters = prereqConf.getParameters() == null ? defaultParameters : prereqConf.getParameters();

        if (dependencies == null) {
            final DependencyConf dependencyConf = new DependencyConf(parameters);
            dependencyConf.setLocks(prereqConf.getLocks());
            return dependencyConf;
        }

        DatamartDependency[] datamarts = dependencies.getDatamarts();
        CommonDependency[] datamartDependencies = getCommonDependencies(datamarts);
        CommonDependency[] replicasDependencies = dependencies.getReplicas();
        String awaitStrategy = prereqConf.getEventAwaitStrategy();
        String eventAwaitStrategy = (awaitStrategy == null) ? AND_EVENT_AWAIT_SREATEGY : awaitStrategy;
        String cronExpression = dependencies.getCron();
        boolean cronActive = true;
        if (("* * * * *").equals(cronExpression)) {
            cronActive = false;
        }
        Cron cron = (cronExpression == null) ? null : new Cron(cronExpression, cronActive);
        final DependencyConf dependencyConf = new DependencyConf(replicasDependencies, datamartDependencies, parameters, eventAwaitStrategy, cron);
        dependencyConf.setLocks(prereqConf.getLocks());
        return dependencyConf;
    }

    private Optional<PrereqConf> findConfig(String name) {
        return Arrays.stream(prereqConfs)
                .filter(prereqConf -> prereqConf.getName().equals(name))
                .findAny();
    }

    private CommonDependency[] getCommonDependencies(DatamartDependency[] datamarts) {
        if (datamarts != null) {
            return Arrays.stream(datamarts)
                    .map(datamartDependency -> new CommonDependency(datamartDependency.getStatId(),
                            null,
                            getCtlEntities(datamartDependency.getEntities()))
                    )
                    .toArray(CommonDependency[]::new);
        }
        return new CommonDependency[]{};
    }

    private Integer[] getCtlEntities(String[] entities) {
        return Arrays.stream(entities)
                .map(this::ctlEntityId)
                .toArray(Integer[]::new);
    }

    private Integer ctlEntityId(String entityId) {
        final DatamartProperties propertiesOfDm = new DatamartProperties(FullTableName.of(entityId), customProperties);
        return propertiesOfDm.getCtlEntityId()
                .orElseThrow(() -> new IllegalStateException("Couldn't find ctlEntityId for " + entityId));
    }
}
