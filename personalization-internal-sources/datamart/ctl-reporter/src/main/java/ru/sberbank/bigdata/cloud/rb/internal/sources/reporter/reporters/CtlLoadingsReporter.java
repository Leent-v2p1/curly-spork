package ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.reporters;

import lombok.SneakyThrows;
import org.json.JSONException;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.CtlApiCalls;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.CtlApiCallsV1;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.dto.LoadingResponse;
import ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.Ctl;
import ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.WorkflowReader;
import ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.entitities.CtlLoadingInfo;
import ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.entitities.CtlWorkflowInfo;
import ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.reports.CsvLoadingReport;
import ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.reports.Report;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Утилита для создания отчёта по загрузкам ctl по потокам, полученных при помощи утилиты CtlWorkflowLoader
 *
 * Пример запуска:
 * java -Doutput.path=C:\some\path -Dctl.url=http://hw2288-05.od-dev.omega.sbrf.ru:8080 -jar reporter.jar
 */
public class CtlLoadingsReporter {

    public static final DateTimeFormatter CTL_TIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.0");
    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private final CtlApiCallsV1 ctlApiCalls;

    CtlLoadingsReporter(CtlApiCallsV1 ctlApiCalls) {
        this.ctlApiCalls = ctlApiCalls;
    }

    /**
     * CtlWorkflowReporter should be used before this reporter and required worklowfs should be marked as used in generated workflows file
     */
    public static void main(String[] args) throws IOException {
        final LocalDate startRunningAfter = LocalDate.now().minusDays(1);
        final LocalDate endedBefore = LocalDate.now();
        Ctl ctl = Ctl.PROD;

        //init ctlLoadingsReporter
        String defaultOutputPath = Paths.get(System.getProperty("user.home"), "CTL_reports", ctl.name()).toAbsolutePath().toString();
        final Path pathToReport = Paths.get(System.getProperty("output.path", defaultOutputPath));
        final String ctlUrl = System.getProperty("ctl.url", ctl.getCtlUrl());
        CtlApiCallsV1 ctlApiCalls = new CtlApiCallsV1(ctlUrl);
        CtlLoadingsReporter ctlLoadingsReporter = new CtlLoadingsReporter(ctlApiCalls);

        //read workflows from file
        Map<Integer, CtlWorkflowInfo> workflows = new WorkflowReader().readWorkflowsFromFile(pathToReport);

        //collect loadings
        List<CtlLoadingInfo> report = ctlLoadingsReporter.createReport(workflows, startRunningAfter, endedBefore);

        //create csv report
        Report csvReport = new CsvLoadingReport(pathToReport, startRunningAfter, endedBefore);
        csvReport.generateReport(report);
    }

    /**
     * Create empty loading for workflows without loadings in ctl
     */
    private CtlLoadingInfo emptyLoading(CtlWorkflowInfo wf, LocalDate reportDate) {
        return new CtlLoadingInfo(
                null,
                wf.id,
                wf.system,
                wf.workflowName,
                null,
                null,
                null,
                null,
                null,
                null,
                reportDate.toString()
        );
    }

    /**
     * Build timeline of runs by explosion report period
     */
    protected Stream<CtlLoadingInfo> explodeLoading(CtlLoadingInfo loadingInfo, LocalDate startFrom, LocalDate endedBefore) {
        int days = startFrom.until(endedBefore).getDays() + 1;
        return Stream.iterate(startFrom, date -> date.plusDays(1))
                .limit(days)
                .filter(date -> {
                    final String loadingDate = getReportDate(loadingInfo.startRunning, loadingInfo.startLoading);
                    return loadingDate != null && !date.isBefore(LocalDate.parse(loadingDate));
                })
                .map(loadingInfo::copyWithReportDate);
    }

    private CtlLoadingInfo resultRow(CtlWorkflowInfo workflow, LoadingResponse loading, String startRunningTime) {
        return new CtlLoadingInfo(
                String.valueOf(loading.id),
                String.valueOf(loading.wf_id),
                workflow.system,
                workflow.workflowName,
                loading.alive,
                loading.status,
                loading.start_dttm,
                startRunningTime,
                loading.end_dttm,
                calculateDuration(startRunningTime, loading.end_dttm),
                getReportDate(startRunningTime, loading.start_dttm));
    }

    private String calculateDuration(String start, String end) {
        if (start == null || end == null) {
            return "";
        }
        final LocalDateTime startTime = LocalDateTime.parse(start, CTL_TIME_FORMAT);
        final LocalDateTime endTime = LocalDateTime.parse(end, CTL_TIME_FORMAT);
        final Duration timeBetween = Duration.between(startTime, endTime);
        final long hours = timeBetween.toHours();
        final long minutes = timeBetween.toMinutes() % 60;
        return hours + ":" + minutes;
    }

    private String getReportDate(String startRunning, String startLoading) {
        if (startRunning != null) {
            String[] dateTime = startRunning.split(" ");
            if (dateTime[0].length() > 0) {
                return dateTime[0];
            }
        }
        if (startLoading != null) {
            String[] dateTime = startLoading.split(" ");
            if (dateTime[0].length() > 0) {
                return dateTime[0];
            }
        }
        return null;
    }

    /**
     * Method gets loading:
     * with 'status' = SUCCESS and filtered by startFrom and endedBefore and
     * with 'alive' = 'ACTIVE' and 'status' = 'EVENT-WAIT', 'TIME-WAIT', 'ERROR', 'RUNNING'
     * then combines them, map to Result entity, sort them with compareTo of Result class and then returns it as list
     */
    @SneakyThrows
    List<CtlLoadingInfo> createReport(Map<Integer, CtlWorkflowInfo> workflows, LocalDate startFrom, LocalDate endedBefore) {
        //get loading with 'alive' = 'ACTIVE' and 'status' = 'EVENT-WAIT', 'TIME-WAIT', 'ERROR', 'RUNNING'
        Stream<CtlLoadingInfo> activeLoadings = workflows.keySet()
                .stream()
                .flatMap(wfId -> ctlApiCalls.getActiveLoadings(wfId).stream())
                .map(loading -> {
                    final String startRunningTime;
                    try {
                        startRunningTime = startRunningTime(loading);
                    } catch (JSONException e) {
                        throw new RuntimeException(e);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    final CtlWorkflowInfo workflow = workflows.get(loading.wf_id);
                    return resultRow(workflow, loading, startRunningTime);
                })
                .flatMap(loadingInfo -> explodeLoading(loadingInfo, startFrom, endedBefore));

        //get loading with 'status' = SUCCESS and filtered by startFrom and endedBefore
        Stream<CtlLoadingInfo> successfulLoadings = workflows.keySet()
                .stream()
                .flatMap(wfId -> ctlApiCalls.getLoadingEndedInTime(endedBefore, wfId).stream())//get loadings ended before endedBefore
                .map(loading -> {
                    final String startRunningTime;
                    try {
                        startRunningTime = startRunningTime(loading);
                    } catch (JSONException e) {
                        throw new RuntimeException(e);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    final CtlWorkflowInfo workflow = workflows.get(loading.wf_id);
                    return resultRow(workflow, loading, startRunningTime);
                })
                //filter loadings that starts running after startFrom
                .filter(loadingInfo -> LocalDateTime.parse(loadingInfo.startRunning, CTL_TIME_FORMAT).isAfter(startFrom.atStartOfDay()));

        final List<CtlLoadingInfo> activeAndEndedLoadings = Stream.concat(activeLoadings, successfulLoadings)
                .sorted(Comparator.reverseOrder())
                .collect(Collectors.toList());

        final Set<Integer> collectedLoadingIds = activeAndEndedLoadings.stream()
                .map(ctlLoadingInfo -> ctlLoadingInfo.wfId)
                .map(Integer::valueOf)
                .collect(Collectors.toSet());

        final List<CtlLoadingInfo> workflowsWithoutLoadings = workflows.keySet()
                .stream()
                .filter(wfId -> !collectedLoadingIds.contains(wfId))
                .map(workflowId -> emptyLoading(workflows.get(workflowId), endedBefore))
                .collect(Collectors.toList());
        activeAndEndedLoadings.addAll(workflowsWithoutLoadings);
        return activeAndEndedLoadings;
    }

    private String startRunningTime(LoadingResponse loading) throws JSONException, IOException {
        return ctlApiCalls
                .getLoading(loading.id)
                .loading_status.stream()
                .filter(status -> status.status.equals("RUNNING"))
                .findAny()
                .map(status -> status.effective_from)
                .map(runningTime -> LocalDateTime.parse(runningTime.split("\\.")[0], TIME_FORMAT))
                .map(runningTime -> runningTime.format(CTL_TIME_FORMAT))
                .orElse(null);
    }
}
