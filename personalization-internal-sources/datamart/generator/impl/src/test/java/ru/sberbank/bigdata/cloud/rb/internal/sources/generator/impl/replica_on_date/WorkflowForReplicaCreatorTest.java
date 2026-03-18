package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.replica_on_date;

import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.DatamartProperties;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.WorkflowWithResources;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.MemoryPreset;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.source.workflow.ACTION;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.source.workflow.FORK;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.source.workflow.WORKFLOWAPP;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans.Action;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans.ActionImpl;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans.Branch;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans.Fork;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.write.FileContent;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.replica_on_date.configuration.ReplicaConf;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.replica_on_date.configuration.ReplicaExtractor;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class WorkflowForReplicaCreatorTest {

    @Test
    void createWorkflowTest() {
        //given
        FullTableName testDatamart = FullTableName.of("test_schema", "test_table");
        ForkCreator forkCreator = mock(ForkCreator.class);
        String replicaTable1 = "test_schema_stg.replica_table_1";
        String replicaTable2 = "test_schema_stg.replica_table_2";
        Branch branch1 = new Branch(singletonList(new ActionImpl(replicaTable1)));
        Branch branch2 = new Branch(singletonList(new ActionImpl(replicaTable2)));
        Action fork = new Fork("fork-0", Arrays.asList(branch1, branch2));
        when(forkCreator.createForkWithMaxSize(any(), anyInt())).thenReturn(singletonList(fork));

        DatamartProperties datamartConfig = createPropertiesService(testDatamart);

        ReplicaExtractor replicaExtractor = createReplicaExtractor(replicaTable1, replicaTable2);

        WorkflowForReplicaCreator workflowForReplicaCreator = new WorkflowForReplicaCreator(forkCreator, replicaExtractor, Environment.PRODUCTION);

        //when
        WorkflowWithResources workflowWithShell = workflowForReplicaCreator.create(datamartConfig);

        WORKFLOWAPP workflow = workflowWithShell.workflow;
        //then
        checkWorkflow(workflow);

        List<FileContent> shellList = workflowWithShell.fileContentList;
        checkShellList(shellList);
    }

    private void checkShellList(List<FileContent> fileContentList) {
        assertEquals(6, fileContentList.size());
        FileContent shell1 = fileContentList.get(0);
        FileContent sparkProperties1 = fileContentList.get(2);
        FileContent systemProperties1 = fileContentList.get(3);

        FileContent shell2 = fileContentList.get(1);
        FileContent sparkProperties2 = fileContentList.get(4);
        FileContent systemProperties2 = fileContentList.get(5);

        assertEquals("test-schema-stg-replica-table-1-runner.sh", shell1.fileName());
        assertEquals("test-schema-stg-replica-table-1-properties-spark.template", sparkProperties1.fileName());
        assertEquals("test-schema-stg-replica-table-1-properties-system.template", systemProperties1.fileName());
        assertEquals("test-schema-stg-replica-table-2-runner.sh", shell2.fileName());
        assertEquals("test-schema-stg-replica-table-2-properties-spark.template", sparkProperties2.fileName());
        assertEquals("test-schema-stg-replica-table-2-properties-system.template", systemProperties2.fileName());

        assertTrue(shell1.content()
                .contains("--class ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.runtime.history.RecoveredReplicaTableCreator"));
        assertTrue(shell1.content().contains("--properties-file test-schema-stg-replica-table-1-properties-system.conf"));

        assertTrue(sparkProperties1.content().contains("executorMemory=8G"));
        assertTrue(sparkProperties1.content().contains("executorCoreNum=1"));
        assertTrue(sparkProperties1.content().contains("executors=80"));

        assertTrue(systemProperties1.content().contains("spark.recovery.date=$[recoveryDate]"));
        assertTrue(systemProperties1.content().contains("spark.datamart.parent.table.name=test_schema.test_table"));
        assertTrue(systemProperties1.content().contains("spark.datamart.history.table.name=test_schema_stg.replica_table_1"));

        assertTrue(shell2.content()
                .contains("--class ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.runtime.history.RecoveredReplicaTableCreator"));
        assertTrue(shell2.content().contains("--properties-file test-schema-stg-replica-table-2-properties-system.conf"));

        assertTrue(sparkProperties2.content().contains("executorMemory=8G"));
        assertTrue(sparkProperties2.content().contains("executorCoreNum=1"));
        assertTrue(sparkProperties2.content().contains("executors=80"));

        assertTrue(systemProperties2.content().contains("spark.recovery.date=$[recoveryDate]"));
        assertTrue(systemProperties2.content().contains("spark.datamart.parent.table.name=test_schema.test_table"));
        assertTrue(systemProperties2.content().contains("spark.datamart.history.table.name=test_schema_stg.replica_table_2"));
    }

    private DatamartProperties createPropertiesService(FullTableName testDatamart) {
        DatamartProperties datamartConfig = mock(DatamartProperties.class);
        when(datamartConfig.getTargetTable()).thenReturn(testDatamart.tableName());
        when(datamartConfig.getTargetSchema()).thenReturn(testDatamart.dbName());
        when(datamartConfig.getSourceReplicaTables()).thenReturn((Arrays.asList("replica_table_1", "replica_table_2")));
        return datamartConfig;
    }

    private ReplicaExtractor createReplicaExtractor(String replicaTable1, String replicaTable2) {
        ReplicaExtractor replicaExtractor = mock(ReplicaExtractor.class);
        HashMap<String, ReplicaConf> replicaConf = new HashMap<>();
        replicaConf.put(replicaTable1, new ReplicaConf(replicaTable1, "", MemoryPreset.HIGH));
        replicaConf.put(replicaTable2, new ReplicaConf(replicaTable2, "", MemoryPreset.HIGH));
        when(replicaExtractor.getReplicaTableConfigurations(any(), any())).thenReturn(replicaConf);
        return replicaExtractor;
    }

    private void checkWorkflow(WORKFLOWAPP workflow) {
        assertEquals("replica-for-test-schema-test-table", workflow.getName());
        String expectedActionName1 = "test-schema-stg-replica-table-1";
        String expectedActionName2 = "test-schema-stg-replica-table-2";
        //assert workflow has 2 ACTION
        List<ACTION> sparkAction = getObjects(workflow, action -> action instanceof ACTION, action -> (ACTION) action);
        assertThat(sparkAction, hasSize(2));
        assertEquals(expectedActionName1, sparkAction.get(0).getName());
        assertEquals(expectedActionName2, sparkAction.get(1).getName());

        //assert fork has 2 expected actions
        List<FORK> forks = getObjects(workflow, action -> action instanceof FORK, action -> (FORK) action);
        assertThat(forks, hasSize(1));
        assertThat(forks.get(0).getPath(), hasSize(2));
        assertEquals(expectedActionName1, forks.get(0).getPath().get(0).getStart());
        assertEquals(expectedActionName2, forks.get(0).getPath().get(1).getStart());
    }

    private <T> List<T> getObjects(WORKFLOWAPP workflowapp, Predicate<Object> objectPredicate, Function<Object, T> objectFORKFunction) {
        return workflowapp.getDecisionOrForkOrJoin().stream()
                .filter(objectPredicate)
                .map(objectFORKFunction)
                .collect(Collectors.toList());
    }
}