package ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.reporters;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.CtlCategory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.CtlApiCalls;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.CtlApiCallsV1;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.dto.Category;
import ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.Ctl;
import ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.SourceNameParser;
import ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.entitities.CtlWorkflowInfo;
import ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.reports.CsvWorkflowReport;
import ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.reports.Report;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Утилита для получения списка всех потоков из категорий, представленных в CtlCategory, и записи их в файл
 * Пример запуска:
 * java -Doutput.path=C:\some\path -Dctl.url=http://hw2288-05.od-dev.omega.sbrf.ru:8080 -jar reporter.jar
 *
 */
public class CtlWorkflowReporter {

    private static final String IS_USED_DEFAULT = "0";
    private CtlApiCallsV1 ctlApiCalls;

    public CtlWorkflowReporter(CtlApiCallsV1 ctlApiCalls) {
        this.ctlApiCalls = ctlApiCalls;
    }

    List<CtlWorkflowInfo> getWorkflows() {
        SourceNameParser nameParser = new SourceNameParser();
        return getMasspersCategories()
                .stream()
                .flatMap(category -> ctlApiCalls.getWorkflowsFromCategory(category).stream())
                .filter(wf -> !wf.deleted)
                .map(wf -> new CtlWorkflowInfo(String.valueOf(wf.id), nameParser.resolveWfSystem(wf.name), wf.name, IS_USED_DEFAULT))
                .collect(Collectors.toList());
    }

    List<Integer> getMasspersCategories() {
        Set<String> masspersCategories = CtlCategory.setOfValues();
        masspersCategories.remove(CtlCategory.PERSONALIZATION_INTERNAL_VERIFICATION.value());
        return ctlApiCalls.getAllCategories()
                .stream()
                .filter(category -> masspersCategories.contains(category.getName()))
                .map(Category::getId)
                .collect(Collectors.toList());
    }

    public static void main(String[] args) throws IOException {
        Ctl ctl = Ctl.PROD;

        //init CtlWorkflowLoader
        String defaultOutputPath = Paths.get(System.getProperty("user.home"), "CTL_reports", ctl.name()).toAbsolutePath().toString();
        final Path pathToReport = Paths.get(System.getProperty("output.path", defaultOutputPath));
        final String ctlUrl = System.getProperty("ctl.url", ctl.getCtlUrl());
        CtlApiCallsV1 ctlApiCalls = new CtlApiCallsV1(ctlUrl);
        CtlWorkflowReporter ctlLoadingsReporter = new CtlWorkflowReporter(ctlApiCalls);

        //get workflows from masspers categories
        List<CtlWorkflowInfo> workflows = ctlLoadingsReporter.getWorkflows();

        //create report
        Report report = new CsvWorkflowReport(pathToReport);
        report.generateReport(workflows);
    }
}
