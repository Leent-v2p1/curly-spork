package ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.reports;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.entitities.CtlStatisticInfo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

class CsvStaticReportTest {

    private Path csvReportPath;

    @BeforeEach
    void setUp() throws IOException {
        csvReportPath = Files.createTempDirectory("csv_report");
    }

    @Test
    void generateReport() throws IOException {
        String reportFileName = "file_name.csv";
        CsvStatisticReport loadingReport = spy(new CsvStatisticReport(csvReportPath, null, null));
        doReturn(reportFileName).when(loadingReport).reportFileName(any(), any(), any(), any());
        List<CtlStatisticInfo> ctlStatisticInfos = Arrays.asList(
                new CtlStatisticInfo("1000", "2020-06-22T14:58Z", "1", "default", "init"),
                new CtlStatisticInfo("1001", "2020-06-23T14:58Z", "1", "default", "inc")
        );
        loadingReport.generateReport(ctlStatisticInfos);

        List<String> csvContent = Files.readAllLines(csvReportPath.resolve(reportFileName));

        assertThat(csvContent, hasSize(3));
        assertEquals("loading_id;publication_date;entity_id;profile;statistic_value", csvContent.get(0));
        assertEquals("1000;2020-06-22T14:58Z;1;default;init", csvContent.get(1));
        assertEquals("1001;2020-06-23T14:58Z;1;default;inc", csvContent.get(2));
    }

    @Test
    void reportFileNameStatistic() {
        String reportFileName = new CsvStatisticReport(null, null, null).reportFileName(
                LocalDateTime.of(2000, 1, 1, 0, 0),
                LocalDate.of(2010, 1, 1),
                LocalDate.of(2010, 1, 2),
                ".html"
        );
        assertEquals("2000-01-01-00.00.00_startFrom=2010-01-01_endTo=2010-01-02_statistic_report.html", reportFileName);
    }
}