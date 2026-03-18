package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.production.test;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.BuildProperties;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.copier.ResourceCopier;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.production.copier.TestProductionCreator;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.replica_on_date.ReplicaOnDateGenerator;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.runner.WorkflowGeneratorRunner;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.wfs.WfsGeneratorRunner;

import java.io.IOException;

public class ProductionTestBuilder {
    public static void build() throws IOException {
        Environment environment = Environment.PRODUCTION_TEST;
        final String stageFolder = "oozie/production";
        final String baseAppPath = "oozie-app/wf/custom/rb/production_test";

        final BuildProperties buildProperties = new BuildProperties();
        buildProperties.setBuildDirSysProperty();

        generateReplicaOnDate(environment);
        generateWorkflows(environment);
        WfsGeneratorRunner.generateWfsYaml(environment, buildProperties.projectBuildDirectory.resolve(stageFolder).resolve("wf"));

        new TestProductionCreator(buildProperties.projectBuildDirectory).run(environment);

        ResourceCopier.copy(buildProperties.buildTimestamp, buildProperties.projectBaseDir, stageFolder, baseAppPath);
    }

    private static void generateReplicaOnDate(Environment environment) {
        ReplicaOnDateGenerator generator = new ReplicaOnDateGenerator(environment);
        generator.init();
        generator.generateAndWriteWorkflows();
    }

    private static void generateWorkflows(Environment environment) {
        WorkflowGeneratorRunner runner = new WorkflowGeneratorRunner(environment);
        runner.generateAllWorkflowXml();
    }
}
