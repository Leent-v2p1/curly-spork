package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.write;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.source.workflow.WORKFLOWAPP;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;

class WorkflowWriterTest {

    private Path baseTempDir;

    @BeforeEach
    void setUp() throws IOException {
        this.baseTempDir = Files.createTempDirectory("workflow_writer_test");
    }

    @Test
    void workflowWriterTest() throws IOException {
        //given
        WorkflowWriter workflowWriter = new WorkflowWriter();
        Path fullPath = baseTempDir.resolve("way4").resolve("card");
        WORKFLOWAPP workflowapp = new WORKFLOWAPP();
        workflowapp.setName("workflow-test-name");
        //when
        workflowWriter.writeWorkflow(fullPath, workflowapp);
        //then
        List<String> strings = Files.readAllLines(fullPath.resolve(WorkflowWriter.WORKFLOW_NAME));
        assertThat(strings, hasSize(2));
    }

    @AfterEach
    void tearDown() throws IOException {
        FileUtils.deleteDirectory(baseTempDir.toFile());
    }
}