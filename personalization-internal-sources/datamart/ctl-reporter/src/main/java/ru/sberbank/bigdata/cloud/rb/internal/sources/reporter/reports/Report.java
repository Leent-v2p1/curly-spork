package ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.reports;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

public abstract class Report {

    static final String CSV_SEPARATOR = ";";
    static final String CSV_FILE_EXTENSION = ".csv";

    String reportFileName(LocalDateTime currentTime, LocalDate startFrom, LocalDate startTo, String fileExtension) {
        return String.join("",
                currentTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd-HH.mm.ss")),
                "_startFrom=" + startFrom.toString(),
                "_startTo=" + startTo.toString(),
                "_loading_report",
                fileExtension);
    }

    String reportFileName(LocalDateTime currentTime, String fileExtension) {
        return String.join("",
                currentTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd-HH.mm.ss")),
                "_workflows",
                fileExtension);
    }

    public abstract void generateReport(List<?> loadings) throws IOException;
}
