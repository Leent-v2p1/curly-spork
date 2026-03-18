package ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.reports;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.entitities.CtlWorkflowInfo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

class CsvWorkflowReportTest {

    private Path csvReportPath;

    @BeforeEach
    void setUp() throws IOException {
        csvReportPath = Files.createTempDirectory("csv_report");
    }

    @Test
    void generateReport() throws IOException {
        String reportFileName = "file_name.csv";
        CsvWorkflowReport workflowReport = spy(new CsvWorkflowReport(csvReportPath));
        doReturn(reportFileName).when(workflowReport).reportFileName(any(), any());
        List<CtlWorkflowInfo> ctlWorklowInfos = Arrays.asList(
                new CtlWorkflowInfo("1", "1", "1", "-"),
                new CtlWorkflowInfo("2", "2", "2", "-")
        );
        workflowReport.generateReport(ctlWorklowInfos);

        List<String> csvContent = Files.readAllLines(csvReportPath.resolve(reportFileName));

        assertThat(csvContent, hasSize(3));
        assertEquals("wf_id;wf_system;wf_name;is_used", csvContent.get(0));
        assertEquals("1;1;1;-", csvContent.get(1));
        assertEquals("2;2;2;-", csvContent.get(2));
    }
}