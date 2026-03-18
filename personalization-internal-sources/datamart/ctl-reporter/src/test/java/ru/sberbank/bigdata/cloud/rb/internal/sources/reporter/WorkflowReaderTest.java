package ru.sberbank.bigdata.cloud.rb.internal.sources.reporter;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.*;
import ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.entitities.CtlWorkflowInfo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class WorkflowReaderTest {

    private Path tempDirectory;

    @BeforeEach
    void before() throws IOException {
        tempDirectory = Files.createTempDirectory("temp-dir");
    }

    @AfterEach
    void after() throws IOException {
        FileUtils.deleteDirectory(tempDirectory.toFile());
    }

    @Test
    void readWorkflowsFromFileTest() throws IOException {
        Path wfReport = Files.createFile(tempDirectory.resolve("2020-03-12-00.00.00_workflows.csv"));
        Files.write(wfReport, Arrays.asList(
                "wf_id;wf_system;wf_name;is_used",
                "1;ekp;masspers-ekp;0",
                "2;cod;masspers-cod;1"));

        WorkflowReader workflowReader = new WorkflowReader();
        Map<Integer, CtlWorkflowInfo> workflows = workflowReader.readWorkflowsFromFile(tempDirectory);

        assertEquals(1, workflows.size());
        assertThat(workflows, hasEntry(2, new CtlWorkflowInfo("2", "cod", "masspers-cod", "1")));
    }

    @Test
    void noUsedWorkflows() throws IOException {
        Path wfReport = Files.createFile(tempDirectory.resolve("2020-03-12-00.00.00_workflows.csv"));
        Files.write(wfReport, Arrays.asList(
                "wf_id;wf_system;wf_name;is_used",
                "1;ekp;masspers-ekp;0")
        );

        assertThrows(IllegalStateException.class, () -> new WorkflowReader().readWorkflowsFromFile(tempDirectory));
    }

    @Nested
    @DisplayName("метод getWorkflowFile()")
    class GetWorkflowFileMethod {

        @Test
        @DisplayName("бросает эксепшен, если по указанному пути нет файла с потоками")
        void folderIsEmpty() throws IOException {
            tempDirectory = Files.createTempDirectory("temp-dir");
            Files.createFile(tempDirectory.resolve("report.csv"));
            WorkflowReader workflowReader = new WorkflowReader();

            String expectedMessage = "There is no file with workflows in " + tempDirectory.toString();
            IllegalStateException exception = assertThrows(IllegalStateException.class,
                    () -> workflowReader.getWorkflowFile(tempDirectory));
            assertEquals(expectedMessage, exception.getMessage());
        }

        @Test
        @DisplayName("возвращает путь до самого нового файла с потоками, если он есть")
        void fileExists() throws IOException {
            tempDirectory = Files.createTempDirectory("temp-dir");
            Files.createFile(tempDirectory.resolve("2020-03-12-00.00.00_workflows.csv"));
            Files.createFile(tempDirectory.resolve("2020-03-18-00.00.00_report.csv"));
            Path expectedFile = Files.createFile(tempDirectory.resolve("2020-03-17-00.00.00_workflows.csv"));

            WorkflowReader workflowReader = new WorkflowReader();
            assertEquals(expectedFile, workflowReader.getWorkflowFile(tempDirectory));
        }

        @Test
        @DisplayName("бросает эксепшен, если по указанному пути нет файла с потоками")
        void incorrectPath() {
            Path wrongPath = Paths.get("some_path");
            WorkflowReader workflowReader = new WorkflowReader();
            NoSuchFileException exception = assertThrows(NoSuchFileException.class, () -> workflowReader.getWorkflowFile(wrongPath));
            assertEquals("some_path", exception.getMessage());
        }
    }
}