package ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.reporters;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.CtlApiCalls;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.CtlApiCallsV1;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.dto.Category;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.dto.Workflow;
import ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.entitities.CtlWorkflowInfo;

import java.util.List;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CtlWorkflowReporterTest {

    private static final CtlApiCallsV1 ctlApiCalls = mock(CtlApiCallsV1.class);

    @Test
    @DisplayName("из категорий потоков получаем только те, которые представлены в CtlCategory, кроме 'Personalization internal verification'")
    void getMasspersCategoriesTest() {
        CtlWorkflowReporter workflowReporter = new CtlWorkflowReporter(ctlApiCalls);
        mockGetAllCategories();
        List<Integer> masspersCategories = workflowReporter.getMasspersCategories();

        assertThat(masspersCategories, hasSize(2));
        assertThat(masspersCategories, containsInAnyOrder(12, 50));
    }

    @Test
    @DisplayName("получаем список поток из категорий")
    void getWorkflowsTest() {
        CtlWorkflowReporter workflowReporter = new CtlWorkflowReporter(ctlApiCalls);
        mockGetAllCategories();
        mockGetWorkflowsFromCategory();
        workflowReporter.getWorkflows();
        List<CtlWorkflowInfo> actual = workflowReporter.getWorkflows();
        CtlWorkflowInfo[] expected = {
                new CtlWorkflowInfo("1", "ekp", "masspers-clsklrozn-ekp-oper-daily-datamarts", "0"),
                new CtlWorkflowInfo("3", "triggers", "masspers-clsklrozn-risk-triggers-daily-datamarts", "0"),
                new CtlWorkflowInfo("5", "nba", "masspers-clsklrozn-nba-counts-streaming", "0")
        };

        assertEquals(3, actual.size());
        assertThat(actual, containsInAnyOrder(expected));
    }

    private void mockGetAllCategories() {
        List<Category> categories = asList(
   );
        when(ctlApiCalls.getAllCategories()).thenReturn(categories);
    }

    private void mockGetWorkflowsFromCategory() {
        List<Workflow> wfFrom12Category = asList(
                new Workflow(1, "masspers-clsklrozn-ekp-oper-daily-datamarts", "DHP2 Personalization internal", false),
                new Workflow(2, "masspers-clsklrozn-way4-daily-datamarts", "DHP2 Personalization internal", true)
        );

        List<Workflow> wfFrom50Category = asList(
                new Workflow(3, "masspers-clsklrozn-risk-triggers-daily-datamarts", "DHP2 Personalization internal", false),
                new Workflow(4, "masspers-clsklrozn-cod-55-monthly-datamartswfSystem", "DHP2 Personalization internal", true),
                new Workflow(5, "masspers-clsklrozn-nba-counts-streaming", "DHP2 Personalization internal", false)
        );
        when(ctlApiCalls.getWorkflowsFromCategory(12)).thenReturn(wfFrom12Category);
        when(ctlApiCalls.getWorkflowsFromCategory(50)).thenReturn(wfFrom50Category);
    }
}