package ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.reporters;

import lombok.SneakyThrows;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.CtlApiCalls;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.CtlApiCallsV1;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.dto.Statistic;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.SysPropertyTool;
import ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.Ctl;
import ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.entitities.CtlStatisticInfo;
import ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.reports.CsvStatisticReport;
import ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.reports.Report;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId.LAST_LOADED_STAT_ID;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId.LOADING_TYPE;

/**
 * Утилита для получения списка значений статистик за заданный период, и записи их в файл
 * Пример запуска:
 * java -Doutput.path=C:\some\path -Dentities.list=901031022,901031023 -Dctl.url=http://hw2288-05.od-dev.omega.sbrf.ru:8080 -cp reporter.jar ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.reporters.CtlStatisticReporter
 */
public class CtlStatisticReporter {

    private static final int STATISTIC_TO_GET = LOADING_TYPE.getCode();
    private CtlApiCallsV1 ctlApiCalls;

    public CtlStatisticReporter(CtlApiCallsV1 ctlApiCalls) {
        this.ctlApiCalls = ctlApiCalls;
    }

    static List<Integer> entitiesList(String schemas) {
        return Arrays.stream(schemas.split(",")).map(Integer::parseInt).collect(toList());
    }

    List<CtlStatisticInfo> getStatistics(List<Integer> entities, LocalDate fromDate, LocalDate toDate) {
        return entities.stream()
                .flatMap(entity -> getStatisticInfo(fromDate, toDate, entity))
                .collect(toList());
    }

    public static void main(String[] args) throws IOException {
        final LocalDate startRunningAfter = LocalDate.now().withDayOfMonth(1);
        final LocalDate endedBefore = LocalDate.now();
        final List<Integer> entitiesList = entitiesList(SysPropertyTool.safeSystemProperty("entities.list"));
        Ctl ctl = Ctl.PROD;

        //init CtlStatisticReporter
        String defaultOutputPath = Paths.get(System.getProperty("user.home"), "CTL_reports", ctl.name()).toAbsolutePath().toString();
        final Path pathToReport = Paths.get(System.getProperty("output.path", defaultOutputPath));
        final String ctlUrl = System.getProperty("ctl.url", ctl.getCtlUrl());
        CtlApiCallsV1 ctlApiCalls = new CtlApiCallsV1(ctlUrl);
        CtlStatisticReporter ctlStatisticReporter = new CtlStatisticReporter(ctlApiCalls);

        //get statistics for entities
        List<CtlStatisticInfo> statistics = ctlStatisticReporter.getStatistics(entitiesList, startRunningAfter, endedBefore);

        //create report
        Report report = new CsvStatisticReport(pathToReport, startRunningAfter, endedBefore);
        report.generateReport(statistics);
    }

    private CtlStatisticInfo getCtlStatisticInfo(Map<Integer, Statistic> value) {
        final Statistic statistic = value.get(STATISTIC_TO_GET);
        return new CtlStatisticInfo(String.valueOf(statistic.loading_id),
                value.get(LAST_LOADED_STAT_ID.getCode()).value,
                String.valueOf(statistic.entity_id),
                statistic.profile,
                statistic.value);
    }

    private boolean filterByDates(LocalDate fromDate, LocalDate toDate, Map<Integer, Statistic> loading) {
        final LocalDate publicationDate = LocalDate.parse(loading.get(LAST_LOADED_STAT_ID.getCode()).value.substring(0, 10));
        return !publicationDate.isBefore(fromDate) && !publicationDate.isAfter(toDate);
    }

    @SneakyThrows
    private Stream<? extends CtlStatisticInfo> getStatisticInfo(LocalDate fromDate, LocalDate toDate, Integer entity) {
        final Map<Integer, Map<Integer, Statistic>> groupedByLoading = ctlApiCalls
                .getAllStats(entity)
                .stream()
                .filter(st -> st.stat_id == LAST_LOADED_STAT_ID.getCode() || st.stat_id == STATISTIC_TO_GET)
                .collect(Collectors.groupingBy(Statistic::getLoading_id, toMap(Statistic::getStat_id, Function.identity())));

        return groupedByLoading.values()
                .stream()
                .filter(loading -> loading.get(LAST_LOADED_STAT_ID.getCode()) != null && loading.get(STATISTIC_TO_GET) != null)
                .filter(loading -> filterByDates(fromDate, toDate, loading))
                .map(this::getCtlStatisticInfo)
                .sorted(comparing(CtlStatisticInfo::getEntityId).thenComparing(CtlStatisticInfo::getLoadingId));
    }
}
