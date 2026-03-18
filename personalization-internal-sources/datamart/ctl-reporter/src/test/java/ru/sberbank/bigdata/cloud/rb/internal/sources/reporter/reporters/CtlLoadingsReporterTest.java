package ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.reporters;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.CtlApiCalls;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.CtlApiCallsV1;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.dto.LoadingResponse;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.dto.LoadingStatus;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.dto.LoadingWithStatusResponse;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.dto.Workflow;
import ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.entitities.CtlLoadingInfo;
import ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.entitities.CtlWorkflowInfo;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsIn.isIn;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CtlLoadingsReporterTest {

    private static final String IGNORE = "ignore";
    private static final CtlApiCallsV1 ctlApiCalls = mock(CtlApiCallsV1.class);
    private static final LocalDate startFrom = LocalDate.of(2010, 1, 1);
    private static final LocalDate startTo = LocalDate.of(2010, 1, 2);
    private static final CtlLoadingsReporter loadingsReporter = new CtlLoadingsReporter(ctlApiCalls);

    @Test
    @DisplayName("на каждую загрузку в CTL удовлетворяющую фильтрам формирует 1 сущность Result")
    void generatesResultList() {
        //mock ctl calls for ACTIVE: RUNNING loading of wf mdm-daily
        mockRunning();
        //mock ctl calls for ACTIVE: EVENT-WAIT loading of wf mdm-stoplist-daily
        mockEventWait();
        //mock ctl calls for COMPLETED: SUCCESS loading of wf ekp-currency-rate-daily
        mockCompleted();

        List<CtlLoadingInfo> report = loadingsReporter.createReport(createWorkflowsIdMap(), startFrom, startTo);
        assertThat(report, hasSize(6));

        CtlLoadingInfo eventWaitLoading = new CtlLoadingInfo("555",
                "7588",
                "way4",
                "masspers-clsklrozn-way4-currency-daily-datamarts",
                "ACTIVE",
                "EVENT-WAIT",
                "2010-01-01 10:00:00.0",
                null,
                null,
                "",
                "2010-01-01");
        assertEquals(eventWaitLoading.copyWithReportDate(startTo), report.get(0));
        assertEquals(eventWaitLoading, report.get(1));

        CtlLoadingInfo completedLoading = new CtlLoadingInfo("101",
                "7129",
                "mdm",
                "masspers-clsklrozn-mdm-daily-datamarts",
                "COMPLETED",
                "SUCCESS",
                "2010-01-01 12:00:00.0",
                "2010-01-01 12:31:00.0",
                "2010-01-01 15:00:00.0",
                "2:29",
                "2010-01-01");
        assertEquals(completedLoading, report.get(2));

        CtlLoadingInfo activeLoading = new CtlLoadingInfo("100",
                "10001",
                "ekp",
                "masspers-clsklrozn-ekp-currency-rate-daily-datamarts",
                "ACTIVE",
                "RUNNING",
                "2010-01-01 10:00:00.0",
                "2010-01-01 10:01:00.0",
                null,
                "",
                "2010-01-01");
        assertEquals(activeLoading.copyWithReportDate(startTo), report.get(3));
        assertEquals(activeLoading, report.get(4));

        CtlLoadingInfo emptyLoading = new CtlLoadingInfo(
                null,
                "10002",
                "ekp",
                "workflow-without-loadings",
                null,
                null,
                null,
                null,
                null,
                null,
                "2010-01-02");
        assertEquals(emptyLoading, report.get(5));
    }

    private Map<Integer, CtlWorkflowInfo> createWorkflowsIdMap() {
        Map<Integer, CtlWorkflowInfo> workflowsId = new HashMap<>();
        workflowsId.put(7588, new CtlWorkflowInfo("7588", "way4", "masspers-clsklrozn-way4-currency-daily-datamarts", "-"));
        workflowsId.put(7129, new CtlWorkflowInfo("7129", "mdm", "masspers-clsklrozn-mdm-daily-datamarts", "-"));
        workflowsId.put(10001, new CtlWorkflowInfo("10001", "ekp", "masspers-clsklrozn-ekp-currency-rate-daily-datamarts", "-"));
        workflowsId.put(10002, new CtlWorkflowInfo("10002", "ekp", "workflow-without-loadings", "-"));
        return workflowsId;
    }

    private void mockRunning() {
        mockMainActiveLoadings("ACTIVE", 100, 10001, "2010-01-01 10:00:00.0", null, "RUNNING");
        List<LoadingStatus> loadingStatuses = singletonList(new LoadingStatus(100, "2010-01-01 10:01:00.0", "RUNNING", IGNORE));
        LoadingWithStatusResponse loading = getLoadingWithStatusResponse(loadingStatuses, 10001, IGNORE);
        when(ctlApiCalls.getLoading(eq(100))).thenReturn(loading);
    }

    private void mockEventWait() {
        mockMainActiveLoadings("ACTIVE", 555, 7588, "2010-01-01 10:00:00.0", null, "EVENT-WAIT");
        LoadingWithStatusResponse loading = getLoadingWithStatusResponse(emptyList(), 7588, IGNORE);
        when(ctlApiCalls.getLoading(eq(555))).thenReturn(loading);
    }

    private void mockCompleted() {
        mockMainLoadings("COMPLETED", 101, 7129, "2010-01-01 12:00:00.0", "2010-01-01 15:00:00.0", "SUCCESS");
        List<LoadingStatus> loadingStatuses = asList(
                new LoadingStatus(101, "2010-01-01 12:31:00.0", "RUNNING", IGNORE),
                new LoadingStatus(101, "2010-01-01 12:32:00.0", "COMPLETED", IGNORE)
        );
        LoadingWithStatusResponse loading = getLoadingWithStatusResponse(loadingStatuses, 7129, "true");
        when(ctlApiCalls.getLoading(eq(101))).thenReturn(loading);
    }

    private void mockMainLoadings(String active,
                                  int loadingId,
                                  int workflowId,
                                  String startTime,
                                  String endTime, String running) {
        List<LoadingResponse> loadingResponses = singletonList(
                getLoadingResponse(active, loadingId, workflowId, startTime, endTime, running)
        );
        when(ctlApiCalls.getLoadingEndedInTime(any(), eq(workflowId))).thenReturn(loadingResponses);
    }

    private void mockMainActiveLoadings(String active,
                                        int loadingId,
                                        int workflowId,
                                        String startTime,
                                        String endTime, String running) {
        List<LoadingResponse> loadingResponses = singletonList(
                getLoadingResponse(active, loadingId, workflowId, startTime, endTime, running)
        );
        when(ctlApiCalls.getActiveLoadings(eq(workflowId))).thenReturn(loadingResponses);
    }

    private LoadingResponse getLoadingResponse(String alive,
                                               int loadingId,
                                               int workflowId,
                                               String startTime,
                                               String endTime,
                                               String status) {
        return new LoadingResponse(false,
                startTime,
                endTime,
                IGNORE,
                alive,
                loadingId,
                IGNORE,
                status,
                workflowId
        );
    }

    private LoadingWithStatusResponse getLoadingWithStatusResponse(List<LoadingStatus> loadingStatuses, int workflowfId, String alive) {
        return new LoadingWithStatusResponse(false,
                new Workflow(workflowfId, IGNORE, IGNORE, false),
                IGNORE,
                IGNORE,
                IGNORE,
                alive,
                1,
                IGNORE,
                IGNORE,
                workflowfId,
                IGNORE,
                loadingStatuses);
    }

    @Nested
    class TestExplodeLoading {
        @Test
        void correctTime() {
            CtlLoadingInfo loading = new CtlLoadingInfo("100",
                    "10001",
                    "ekp",
                    "masspers-wf-datamarts",
                    "ACTIVE",
                    "RUNNING",
                    "2010-01-02 10:00:00.0",
                    "2010-01-02 10:01:00.0",
                    null,
                    "",
                    "2010-01-02");
            Set<CtlLoadingInfo> loadings = loadingsReporter.explodeLoading(loading, startFrom, startTo).collect(Collectors.toSet());
            assertThat(loadings, hasSize(1));
            assertThat(loading, isIn(loadings));
        }

        @Test
        void nullTime() {
            CtlLoadingInfo loading = new CtlLoadingInfo("100",
                    "10001",
                    "ekp",
                    "masspers-wf-datamarts",
                    "ACTIVE",
                    "RUNNING",
                    null,
                    null,
                    null,
                    "",
                    "2010-01-01");
            Stream<CtlLoadingInfo> loadings = loadingsReporter.explodeLoading(loading, startFrom, startTo);
            assertEquals(0, loadings.count());
        }

        @Test
        void emptyTime() {
            CtlLoadingInfo loading = new CtlLoadingInfo("100",
                    "10001",
                    "ekp",
                    "masspers-wf-datamarts",
                    "ACTIVE",
                    "RUNNING",
                    "",
                    "",
                    null,
                    "",
                    "2010-01-01");
            Stream<CtlLoadingInfo> loadings = loadingsReporter.explodeLoading(loading, startFrom, startTo);
            assertEquals(0, loadings.count());
        }

        @Test
        void incorrectTime() {
            CtlLoadingInfo loading = new CtlLoadingInfo("100",
                    "10001",
                    "ekp",
                    "masspers-wf-datamarts",
                    "ACTIVE",
                    "RUNNING",
                    "2010-01-02T10:00:00.0",
                    "2010-01-02T10:01:00.0",
                    null,
                    "",
                    "2010-01-01");
            DateTimeParseException exception = assertThrows(DateTimeParseException.class,
                    () -> loadingsReporter.explodeLoading(loading, startFrom, startTo).count());
            assertEquals("Text '2010-01-02T10:01:00.0' could not be parsed, unparsed text found at index 10",
                    exception.getMessage());
        }
    }
}