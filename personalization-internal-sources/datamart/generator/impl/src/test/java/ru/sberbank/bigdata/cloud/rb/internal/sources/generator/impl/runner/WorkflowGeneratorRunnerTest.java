package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.runner;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.base.WorkflowType;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.DatamartProperties;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.NameAdditions;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.test_api.DatamartTest;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.file.TargetPathBuilder;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.source.workflow.WORKFLOWAPP;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.ResultActionConf;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.write.FileContent;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.write.FileWriter;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.write.WorkflowWriter;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment.PRODUCTION;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.CommonFiles.PERSONALIZATION_JAR;

class WorkflowGeneratorRunnerTest extends DatamartTest {

    private final WorkflowGeneratorRunner workflowGeneratorRunner
            = new WorkflowGeneratorRunner(null, null, null, null);

    private List<ResultActionConf> getDatamartList(String repartition) {
        final ResultActionConf resultActionConf = new ResultActionConf();
        resultActionConf.setName("name");
        resultActionConf.setType("datamart");
        resultActionConf.setDependencies(Collections.singletonList("name2"));
        final HashMap<String, String> customJavaOpts = new HashMap<>();
        customJavaOpts.put("repartition", repartition);
        resultActionConf.setCustomJavaOpts(customJavaOpts);
        resultActionConf.setJarName("test-datamart.jar");
        return Collections.singletonList(resultActionConf);
    }

    @Nested
    class CreateCtlStatsPublisher {
        @Test
        void generateAction() {
            final List<ResultActionConf> datamart = getDatamartList("enabled");
            final List<ResultActionConf> ctlPublisherActions = workflowGeneratorRunner.createCtlStatsPublisher(datamart);
            assertThat(ctlPublisherActions, hasSize(1));
            final ResultActionConf result = ctlPublisherActions.get(0);
            assertEquals("name_ctl_stats_publisher", result.getName());
            assertEquals("ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.CtlStatisticService", result.getClassName());
            assertEquals("name", result.getWorkflowName());
            assertEquals("name_reserve", result.getDependencies().get(0));
            assertEquals( Collections.singletonList(PERSONALIZATION_JAR), result.getExtraJarNames());
            assertEquals(PERSONALIZATION_JAR, result.getJarName());
            assertFalse(result.isStart());
        }
    }

    private List<ResultActionConf> getDatamartListWithDQCheck(String dqcheck) {
        final ResultActionConf resultActionConf = new ResultActionConf();
        resultActionConf.setName("name");
        resultActionConf.setType("datamart");
        resultActionConf.setDependencies(Collections.singletonList("name2"));
        final HashMap<String, String> customJavaOpts = new HashMap<>();
        customJavaOpts.put("dqcheck", dqcheck);
        resultActionConf.setCustomJavaOpts(customJavaOpts);
        resultActionConf.setJarName("test-datamart.jar");
        return Collections.singletonList(resultActionConf);
    }

    @Nested
    class CreateDQCheck {

        @Test
        void dqcheckDisabled() {
            final List<ResultActionConf> datamart = getDatamartListWithDQCheck("disabled");
            final List<ResultActionConf> dataQualityCheckList = workflowGeneratorRunner.createDataQualityCheck(datamart);
            assertThat(dataQualityCheckList, empty());
        }

        @Test
        void dqcheckEnabled() {
            final List<ResultActionConf> datamart = getDatamartListWithDQCheck("enabled");
            final List<ResultActionConf> dataQualityCheckList = workflowGeneratorRunner.createDataQualityCheck(datamart);
            assertThat(dataQualityCheckList, hasSize(1));
            final ResultActionConf result = dataQualityCheckList.get(0);
            assertEquals("dqcheck", result.getType());
            assertEquals("ru.sberbank.bigdata.cloud.rb.internal.sources.common.service.data_quality_check.DataQualityCheckService", result.getClassName());
            assertEquals("name_dqcheck", result.getName());
            assertEquals("name", result.getWorkflowName());
            assertEquals(Arrays.asList("test-datamart.jar", PERSONALIZATION_JAR), result.getExtraJarNames());
            assertEquals(PERSONALIZATION_JAR, result.getJarName());
            assertFalse(result.isStart());
            assertEquals("name", result.getDependencies().get(0));
        }
    }

    @Nested
    class CreateRepartitioner {

        @Test
        void repartitioningDisabled() {
            final List<ResultActionConf> datamart = getDatamartList("disabled");
            final List<ResultActionConf> repartitionerList = workflowGeneratorRunner.createRepartitioner(datamart);
            assertThat(repartitionerList, empty());
        }

        @Test
        void repartitioningEnabled() {
            final List<ResultActionConf> datamart = getDatamartList("enabled");
            final List<ResultActionConf> repartitionerList = workflowGeneratorRunner.createRepartitioner(datamart);
            assertThat(repartitionerList, hasSize(1));
            final ResultActionConf result = repartitionerList.get(0);
            assertEquals("repartitioner", result.getType());
            assertEquals("ru.sberbank.bigdata.cloud.rb.internal.sources.common.service.repartition.RepartitionService", result.getClassName());
            assertEquals("name_repartitioner", result.getName());
            assertEquals("name", result.getWorkflowName());
            assertEquals(Collections.singletonList(PERSONALIZATION_JAR), result.getExtraJarNames());
            assertEquals(PERSONALIZATION_JAR, result.getJarName());
            assertFalse(result.isStart());
            assertEquals("name", result.getDependencies().get(0));
        }
    }


    @Nested
    class CreateReserving {

        @Test
        void reserving() {
            final List<ResultActionConf> datamart = getDatamartList("disabled");
            final List<ResultActionConf> reserving = workflowGeneratorRunner.createReserving(datamart);
            assertThat(reserving, hasSize(1));
            final ResultActionConf result = reserving.get(0);
            assertEquals("reserve", result.getType());
            assertEquals("ru.sberbank.bigdata.cloud.rb.internal.sources.common.service.reserving.ReservingService", result.getClassName());
            assertEquals("name_reserve", result.getName());
            assertEquals("name", result.getWorkflowName());
            assertEquals(Arrays.asList("test-datamart.jar", PERSONALIZATION_JAR), result.getExtraJarNames());
            assertEquals(PERSONALIZATION_JAR, result.getJarName());
            assertFalse(result.isStart());
            assertEquals("name", result.getDependencies().get(0));
        }

        @Test
        void reservingWithRepartitioner() {
            final List<ResultActionConf> datamart = getDatamartList("enabled");
            final List<ResultActionConf> reserving = workflowGeneratorRunner.createReserving(datamart);
            assertThat(reserving, hasSize(1));
            final ResultActionConf result = reserving.get(0);
            assertEquals("reserve", result.getType());
            assertEquals("ru.sberbank.bigdata.cloud.rb.internal.sources.common.service.reserving.ReservingService", result.getClassName());
            assertEquals("name_reserve", result.getName());
            assertEquals("name", result.getWorkflowName());
            assertEquals(Arrays.asList("test-datamart.jar", PERSONALIZATION_JAR), result.getExtraJarNames());
            assertEquals(PERSONALIZATION_JAR, result.getJarName());
            assertFalse(result.isStart());
            assertEquals("name_repartitioner", result.getDependencies().get(0));
        }
    }


    @Nested
    class createProperties {

        @Test
        void properties() {
            List<ResultActionConf> datamartConfList = getDatamartList("disabled");
            ResultActionConf datamartConf = datamartConfList.get(0);

            List<ResultActionConf> propertiesService = workflowGeneratorRunner.createPropertiesService(datamartConfList);
            assertThat(propertiesService, hasSize(1));
            final ResultActionConf result = propertiesService.get(0);
            assertEquals(WorkflowType.PROPERTIES.getKey(), result.getType());
            assertFalse(result.isStart());
            assertEquals("name2", result.getDependencies().get(0));
            assertEquals(DatamartProperties.class.getName(), result.getClassName());
            assertEquals(Collections.singletonList(PERSONALIZATION_JAR), result.getExtraJarNames());
            assertEquals(PERSONALIZATION_JAR, result.getJarName());

            assertThat(datamartConf.getDependencies(), hasSize(1));
            assertEquals("name" + NameAdditions.PROPERTIES_POSTFIX, datamartConf.getDependencies().get(0));
        }
    }


    @Nested
    class GenerateAllWorkflowXml {

        final ArgumentCaptor<Path> pathCapture = ArgumentCaptor.forClass(Path.class);
        final ArgumentCaptor<WORKFLOWAPP> mainXmlCaptor = ArgumentCaptor.forClass(WORKFLOWAPP.class);
        final ArgumentCaptor<List<FileContent>> fileContentCaptor = ArgumentCaptor.forClass(List.class);
        private final WorkflowWriter workflowWriter = mock(WorkflowWriter.class);
        private final FileWriter fileWriter = mock(FileWriter.class);
        private final TargetPathBuilder pathBuilder = mock(TargetPathBuilder.class);
        private final WorkflowGeneratorRunner generatorRunner = spy(new WorkflowGeneratorRunner(
                PRODUCTION,
                pathBuilder,
                workflowWriter,
                fileWriter
        ));

        @Test
        @DisplayName("проверяет, что генерируются необходимые .xml, .sh и .template файлы для витрины custom_rb_card.card")
        void testGenerationForDatamart() {
            mockPropertiesMethods(generatorRunner, singleton("custom_rb_card.card"));

            final Path expectedWorkflowPath = Paths.get("some_path");
            doReturn(expectedWorkflowPath).when(pathBuilder).resolveGeneratedWorkflowBuildDir(FullTableName.of("custom_rb_card.card"));

            doNothing().when(workflowWriter).writeWorkflow(pathCapture.capture(), mainXmlCaptor.capture());
            doNothing().when(fileWriter).write(any(), fileContentCaptor.capture());

            generatorRunner.generateAllWorkflowXml();

            assertEquals(expectedWorkflowPath, pathCapture.getValue());
            final WORKFLOWAPP xmlContent = mainXmlCaptor.getValue();
            final List<Object> actions = xmlContent.getDecisionOrForkOrJoin();
            assertThat("expecting 3 properties actions, 3 major action(build, reserve, statistics) and kill action", actions, hasSize(7));
            final List<FileContent> files = fileContentCaptor.getValue();
            assertThat("expecting 3 .sh runner for major actions + 6 templates for properties ", files, hasSize(9));
        }

        private void mockPropertiesMethods(WorkflowGeneratorRunner generatorRunner, Set<String> datamartsList) {
            doReturn(datamartsList).when(generatorRunner).propertiesUtilWrapper(WorkflowType.DATAMART);
            doReturn(emptySet()).when(generatorRunner).propertiesUtilWrapper(WorkflowType.HELPER);
            doReturn(emptySet()).when(generatorRunner).propertiesUtilWrapper(WorkflowType.STAGE);
        }
    }
}