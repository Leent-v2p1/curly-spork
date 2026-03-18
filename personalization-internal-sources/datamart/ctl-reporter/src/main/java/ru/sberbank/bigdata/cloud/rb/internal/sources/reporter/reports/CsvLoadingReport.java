package ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.reports;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.entitities.CtlLoadingInfo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CsvLoadingReport extends Report {

    private static final Logger log = LoggerFactory.getLogger(CsvLoadingReport.class);

    private final Path pathToReport;
    private final LocalDate startFrom;
    private final LocalDate startTo;

    public CsvLoadingReport(Path pathToReport, LocalDate startFrom, LocalDate startTo) {
        this.pathToReport = pathToReport;
        this.startFrom = startFrom;
        this.startTo = startTo;
    }

    @Override
    public void generateReport(List<?> loadings) throws IOException {
        String csvContent = loadings.stream()
                .map(result -> (CtlLoadingInfo) result)
                .map(mapToCsvRow())
                .collect(Collectors.joining(System.lineSeparator()));
        String result = csvHeader() + System.lineSeparator() + csvContent;
        Files.createDirectories(pathToReport);

        String reportFileName = reportFileName(LocalDateTime.now(), startFrom, startTo, CSV_FILE_EXTENSION);
        Path fullFileName = pathToReport.resolve(reportFileName);
        Files.write(fullFileName, result.getBytes());
        int numberOfLoadings = loadings.size();
        log.info("Write csv report to directory: {} containing {} loadings", fullFileName, numberOfLoadings);
    }

    private Function<CtlLoadingInfo, String> mapToCsvRow() {
        return ctlLoadingInfo -> String.join(CSV_SEPARATOR,
                ctlLoadingInfo.loadingId,
                ctlLoadingInfo.wfId,
                ctlLoadingInfo.wfSystem,
                ctlLoadingInfo.wfName,
                ctlLoadingInfo.alive,
                ctlLoadingInfo.status,
                ctlLoadingInfo.startLoading,
                ctlLoadingInfo.startRunning,
                ctlLoadingInfo.endLoading,
                ctlLoadingInfo.duration,
                ctlLoadingInfo.reportDate);
    }

    private String csvHeader() {
        return String.join(CSV_SEPARATOR,
                "loading_id",
                "wf_id",
                "wf_system",
                "wf_name",
                "alive",
                "status",
                "start_loading",
                "start_running",
                "end_loading",
                "duration(hh:mm)",
                "report_date");
    }
}
