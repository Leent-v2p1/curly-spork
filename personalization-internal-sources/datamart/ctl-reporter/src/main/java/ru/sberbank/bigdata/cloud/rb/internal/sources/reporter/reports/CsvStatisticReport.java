package ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.reports;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.entitities.CtlStatisticInfo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CsvStatisticReport extends Report {

    private static final Logger log = LoggerFactory.getLogger(CsvStatisticReport.class);

    private final Path pathToReport;
    private final LocalDate startFrom;
    private final LocalDate endTo;

    public CsvStatisticReport(Path pathToReport, LocalDate startFrom, LocalDate endTo) {
        this.pathToReport = pathToReport;
        this.startFrom = startFrom;
        this.endTo = endTo;
    }

    @Override
    public void generateReport(List<?> statisticValues) throws IOException {
        String csvContent = statisticValues.stream()
                .map(result -> (CtlStatisticInfo) result)
                .map(mapToCsvRow())
                .collect(Collectors.joining(System.lineSeparator()));
        String result = csvHeader() + System.lineSeparator() + csvContent;
        Files.createDirectories(pathToReport);

        String reportFileName = reportFileName(LocalDateTime.now(), startFrom, endTo, CSV_FILE_EXTENSION);
        Path fullFileName = pathToReport.resolve(reportFileName);
        Files.write(fullFileName, result.getBytes());
        int numberOfValues = statisticValues.size();
        log.info("Write csv report to directory: {} containing {} statisticValues", fullFileName, numberOfValues);
    }

    private Function<CtlStatisticInfo, String> mapToCsvRow() {
        return result -> String.join(CSV_SEPARATOR,
                result.loadingId,
                result.publicationDate,
                result.entityId,
                result.profile,
                result.statisticValue
        );
    }

    private String csvHeader() {
        return String.join(CSV_SEPARATOR,
                "loading_id",
                "publication_date",
                "entity_id",
                "profile",
                "statistic_value");
    }

    @Override
    String reportFileName(LocalDateTime currentTime, LocalDate startFrom, LocalDate endTo, String fileExtension) {
        return String.join("",
                currentTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd-HH.mm.ss")),
                "_startFrom=" + startFrom.toString(),
                "_endTo=" + endTo.toString(),
                "_statistic_report",
                fileExtension);
    }
}
