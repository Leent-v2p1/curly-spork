package ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.reporters;

import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.CtlApiCalls;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.CtlApiCallsV1;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.dto.Statistic;
import ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.entitities.CtlStatisticInfo;

import java.time.LocalDate;
import java.time.Month;
import java.util.Arrays;
import java.util.List;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId.CHANGE_STAT_ID;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId.LAST_LOADED_STAT_ID;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId.LOADING_TYPE;

class CtlStatisticReporterTest {

    private static final CtlApiCallsV1 ctlApiCalls = mock(CtlApiCallsV1.class);

    @Test
    void getStatisticsTest() {
        CtlStatisticReporter workflowReporter = new CtlStatisticReporter(ctlApiCalls);
        mockGetAllStatistics();

        List<CtlStatisticInfo> actual = workflowReporter.getStatistics(Arrays.asList(1, 2), LocalDate.of(2020, Month.JANUARY, 1), LocalDate.of(2020, Month.DECEMBER, 31));

        CtlStatisticInfo[] expected = {
                new CtlStatisticInfo("1000", "2020-06-22T14:58Z", "1", "default", "init"),
                new CtlStatisticInfo("1001", "2020-06-23T14:58Z", "1", "default", "inc"),
                new CtlStatisticInfo("2000", "2020-07-23T12:13Z", "2", "default", "inc"),
        };

        assertEquals(3, actual.size());
        assertThat(actual, containsInAnyOrder(expected));
    }

    private void mockGetAllStatistics() {
        final int entityId1 = 1;
        final int entityId2 = 2;

        List<Statistic> statistics = asList(
                new Statistic(1000, "default", entityId1, LOADING_TYPE.getCode(), "init"),
                new Statistic(1000, "default", entityId1, LAST_LOADED_STAT_ID.getCode(), "2020-06-22T14:58Z"),
                new Statistic(1000, "default", entityId1, CHANGE_STAT_ID.getCode(), "true"),
                new Statistic(1001, "default", entityId1, LOADING_TYPE.getCode(), "inc"),
                new Statistic(1001, "default", entityId1, LAST_LOADED_STAT_ID.getCode(), "2020-06-23T14:58Z"),
                new Statistic(1002, "default", entityId1, CHANGE_STAT_ID.getCode(), "true"),
                new Statistic(1003, "default", entityId1, LOADING_TYPE.getCode(), "inc"),
                new Statistic(1004, "default", entityId1, LAST_LOADED_STAT_ID.getCode(), "2020-06-25T14:58Z")
        );

        List<Statistic> statistics2 = asList(
                new Statistic(1999, "default", entityId2, LOADING_TYPE.getCode(), "init"),
                new Statistic(1999, "default", entityId2, LAST_LOADED_STAT_ID.getCode(), "2019-09-13T12:13Z"),
                new Statistic(2000, "default", entityId2, LOADING_TYPE.getCode(), "inc"),
                new Statistic(2000, "default", entityId2, LAST_LOADED_STAT_ID.getCode(), "2020-07-23T12:13Z"),
                new Statistic(2002, "default", entityId2, LOADING_TYPE.getCode(), "inc"),
                new Statistic(2002, "default", entityId2, LAST_LOADED_STAT_ID.getCode(), "2021-01-01T12:13Z")
        );

        when(ctlApiCalls.getAllStats(eq(entityId1))).thenReturn(statistics);
        when(ctlApiCalls.getAllStats(eq(entityId2))).thenReturn(statistics2);
    }

    @Test
    void entitiesList() {
        assertEquals(Arrays.asList(1, 2, 3), CtlStatisticReporter.entitiesList("1,2,3"));
    }
}