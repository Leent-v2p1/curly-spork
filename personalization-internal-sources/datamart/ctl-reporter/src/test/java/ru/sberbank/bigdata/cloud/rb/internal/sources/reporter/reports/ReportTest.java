package ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.reports;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.reports.Report.CSV_FILE_EXTENSION;

class ReportTest {

    @Test
    void reportFileNameLoading() {
        String reportFileName = new CsvLoadingReport(null, null, null).reportFileName(
                LocalDateTime.of(2000, 1, 1, 0, 0),
                LocalDate.of(2010, 1, 1),
                LocalDate.of(2010, 1, 2),
                ".html"
        );
        assertEquals("2000-01-01-00.00.00_startFrom=2010-01-01_startTo=2010-01-02_loading_report.html", reportFileName);
    }

    @Test
    void reportFileNameWorkflow() {
        String reportFileName = new CsvWorkflowReport(null).reportFileName(
                LocalDateTime.of(2000, 1, 1, 0, 0),
                CSV_FILE_EXTENSION
        );
        assertEquals("2000-01-01-00.00.00_workflows.csv", reportFileName);
    }
}