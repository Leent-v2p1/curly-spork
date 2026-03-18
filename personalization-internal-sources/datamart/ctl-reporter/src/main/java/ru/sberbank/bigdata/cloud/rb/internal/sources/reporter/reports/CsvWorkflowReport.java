package ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.reports;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.entitities.CtlWorkflowInfo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CsvWorkflowReport extends Report {

    private static final Logger log = LoggerFactory.getLogger(CsvWorkflowReport.class);

    private final Path pathToReport;

    public CsvWorkflowReport(Path pathToReport) {
        this.pathToReport = pathToReport;
    }

    @Override
    public void generateReport(List<?> workflows) throws IOException {
        String csvContent = workflows.stream()
                .map(result -> (CtlWorkflowInfo) result)
                .map(mapToCsvRow())
                .collect(Collectors.joining(System.lineSeparator()));
        String result = csvHeader() + System.lineSeparator() + csvContent;
        Files.createDirectories(pathToReport);

        String reportFileName = reportFileName(LocalDateTime.now(), CSV_FILE_EXTENSION);
        Path fullFileName = pathToReport.resolve(reportFileName);
        Files.write(fullFileName, result.getBytes());
        int numberOfWorkflows = workflows.size();
        log.info("Write csv report to directory: {} containing {} workflows", fullFileName, numberOfWorkflows);
    }

    private Function<CtlWorkflowInfo, String> mapToCsvRow() {
        return result -> String.join(CSV_SEPARATOR,
                result.id,
                result.system,
                result.workflowName,
                result.isUsed
        );
    }

    private String csvHeader() {
        return String.join(CSV_SEPARATOR,
                "wf_id",
                "wf_system",
                "wf_name",
                "is_used");
    }
}
