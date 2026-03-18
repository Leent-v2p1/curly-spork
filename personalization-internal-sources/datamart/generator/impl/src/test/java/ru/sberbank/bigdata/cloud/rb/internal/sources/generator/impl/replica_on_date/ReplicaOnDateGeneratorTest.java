package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.replica_on_date;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.DatamartProperties;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.file.TargetPathBuilder;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.WorkflowWithResources;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.source.workflow.WORKFLOWAPP;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.write.FileContent;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.write.FileWriter;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.write.WorkflowWriter;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.replica_on_date.configuration.HistoricalDatamartsExtractor;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ReplicaOnDateGeneratorTest {

    @Test
    void workflowGeneratorTest() {
        //given
        FullTableName testDatamart = FullTableName.of("test_schema", "dm_1");
        DatamartProperties datamartConfig = mock(DatamartProperties.class);
        when(datamartConfig.getTargetTable()).thenReturn(testDatamart.dbName());
        when(datamartConfig.getSourceSchema()).thenReturn(testDatamart.tableName());
        when(datamartConfig.getSourceReplicaTables()).thenReturn((Arrays.asList("replica_table_1", "replica_table_2")));

        WorkflowForReplicaCreator workflowCreator = mock(WorkflowForReplicaCreator.class);
        WorkflowWithResources emptyWorkflowWithShell = new WorkflowWithResources(Collections.emptyList(), new WORKFLOWAPP());
        when(workflowCreator.create(any())).thenReturn(emptyWorkflowWithShell);

        HistoricalDatamartsExtractor historicalDatamartsExtractor = mock(HistoricalDatamartsExtractor.class);
        HashMap<FullTableName, DatamartProperties> conf = new HashMap<>();
        conf.put(testDatamart, datamartConfig);
        when(historicalDatamartsExtractor.getHistoricalDatamarts()).thenReturn(conf);

        ReplicaOnDateGenerator generator = new ReplicaOnDateGenerator(Environment.PRODUCTION, null, null, null, historicalDatamartsExtractor, workflowCreator, null);
        //when
        Map<FullTableName, WorkflowWithResources> workflows = generator.generateWorkflows();
        //then
        assertNotNull(workflows);
        assertThat(workflows.entrySet(), hasSize(1));
        assertTrue(workflows.containsKey(testDatamart));
    }

    @Test
    void workflowWritesTest() {
        //given
        WorkflowWriter writer = mock(WorkflowWriter.class);
        FileWriter fileWriter = mock(FileWriter.class);

        List<FileContent> shellList = new ArrayList<>();
        shellList.add(new FileContent("shell", "body"));
        shellList.add(new FileContent("shell", "body"));
        shellList.add(new FileContent("shell", "body"));

        Map<FullTableName, WorkflowWithResources> datamarts = new HashMap<>();
        FullTableName dm1 = FullTableName.of("schema_name", "dm_1");
        datamarts.put(dm1, new WorkflowWithResources(shellList, new WORKFLOWAPP()));
        FullTableName dm2 = FullTableName.of("schema_name", "dm_2");
        datamarts.put(dm2, new WorkflowWithResources(shellList, new WORKFLOWAPP()));

        TargetPathBuilder pathBuilder = mock(TargetPathBuilder.class);
        Path pathDm1 = Paths.get("path/for/dm_1");
        Path pathDm2 = Paths.get("path/for/dm_2");
        when(pathBuilder.resolveReplicaOnDatePath(dm1)).thenReturn(pathDm1);
        when(pathBuilder.resolveReplicaOnDatePath(dm2)).thenReturn(pathDm2);

        ReplicaOnDateGenerator generator = new ReplicaOnDateGenerator(Environment.PRODUCTION, writer, fileWriter, null, null, null, pathBuilder);
        //when
        generator.writeWorkflows(datamarts);
        //then
        ArgumentCaptor<Path> pathArgumentCaptor = ArgumentCaptor.forClass(Path.class);
        verify(writer, times(2)).writeWorkflow(pathArgumentCaptor.capture(), any());
        verify(fileWriter, times(2)).write(pathArgumentCaptor.capture(), any());
        List<Path> actualArguments = pathArgumentCaptor.getAllValues();
        assertEquals(pathDm1, actualArguments.get(0));
        assertEquals(pathDm2, actualArguments.get(1));
    }
}