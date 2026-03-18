package ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.reports;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.entitities.CtlLoadingInfo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

class CsvLoadingReportTest {

    private Path csvReportPath;

    @BeforeEach
    void setUp() throws IOException {
        csvReportPath = Files.createTempDirectory("csv_report");
    }

    @Test
    void generateReport() throws IOException {
        String reportFileName = "file_name.csv";
        CsvLoadingReport loadingReport = spy(new CsvLoadingReport(csvReportPath, null, null));
        doReturn(reportFileName).when(loadingReport).reportFileName(any(), any(), any(), any());
        List<CtlLoadingInfo> ctlLoadingInfos = Arrays.asList(
                new CtlLoadingInfo("1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1"),
                new CtlLoadingInfo("2", "2", "2", "2", "2", "2", "2", "2", "2", "2", "2")
        );
        loadingReport.generateReport(ctlLoadingInfos);

        List<String> csvContent = Files.readAllLines(csvReportPath.resolve(reportFileName));

        assertThat(csvContent, hasSize(3));
        assertEquals("loading_id;wf_id;wf_system;wf_name;alive;status;start_loading;start_running;end_loading;duration(hh:mm);report_date", csvContent.get(0));
        assertThatCsvRowContainsOnly(csvContent.get(1), "1");
        assertThatCsvRowContainsOnly(csvContent.get(2), "2");
    }

    private void assertThatCsvRowContainsOnly(String row, String symbol) {
        assertTrue(row.matches("([" + symbol + ";])*"));
    }
}