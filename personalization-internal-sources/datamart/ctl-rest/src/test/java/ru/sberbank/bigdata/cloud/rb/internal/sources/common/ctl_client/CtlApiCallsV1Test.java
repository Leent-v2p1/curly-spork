package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.dto.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
class CtlApiCallsV1Test {

    private final MockWebServer mockServer = new MockWebServer();

    @Test
    void testRunWorkflow() {
        mockServer.enqueue(new MockResponse().setBody("[\"loading_id\", \"887412\"]"));
        final CtlApiCallsV1 ctlApiCalls = new CtlApiCallsV1(mockServer.url("").toString());
        final Integer actualLoadingId =  ctlApiCalls.runWorkflow(8874, 1);
        assertEquals(new Integer(887412), actualLoadingId);
    }

    @Test
    void testGetLoadingEndedInTime() throws IOException {
        enqueueResponseFromFile("src/test/resources/ctl_responses/loading_filtered_response.json");
        final CtlApiCallsV1 ctlApiCalls = new CtlApiCallsV1(mockServer.url("").toString());
        List<LoadingResponse> loadingEndedInTime = ctlApiCalls.getLoadingEndedInTime(LocalDate.of(2010, 1, 1), 1);

        LoadingResponse active = new LoadingResponse(true, "2020-02-18 09:01:37.0", null, null, "ACTIVE", 2242888, null, "EVENT-WAIT", 10403);
        assertThat(loadingEndedInTime, Matchers.hasSize(1));
        assertThat(loadingEndedInTime, Matchers.contains(active));
    }

    @Test
    void testGetActiveLoadings() throws IOException {
        enqueueResponseFromFile("src/test/resources/ctl_responses/loading_filtered_response.json");
        final CtlApiCallsV1 ctlApiCalls = new CtlApiCallsV1(mockServer.url("").toString());
        List<LoadingResponse> loadingEndedInTime = ctlApiCalls.getActiveLoadings(1);

        LoadingResponse active = new LoadingResponse(true, "2020-02-18 09:01:37.0", null, null, "ACTIVE", 2242888, null, "EVENT-WAIT", 10403);
        assertThat(loadingEndedInTime, Matchers.hasSize(1));
        assertThat(loadingEndedInTime, Matchers.contains(active));
    }

    @Test
    void testGetLoading() throws IOException {
        enqueueResponseFromFile("src/test/resources/ctl_responses/loading_response.json");
        final CtlApiCallsV1 ctlApiCalls = new CtlApiCallsV1(mockServer.url("").toString());
        LoadingWithStatusResponse loadingEndedInTime = ctlApiCalls.getLoading(1);

        assertNotNull(loadingEndedInTime);
        assertEquals(7588, loadingEndedInTime.wf_id);
        assertEquals(7, loadingEndedInTime.loading_status.size());
        assertEquals("masspers-clsklrozn-mdm-stoplist-daily-datamarts", loadingEndedInTime.workflow.name);
    }

    private void enqueueResponseFromFile(String file) throws IOException {
        byte[] bytes = Files.readAllBytes(Paths.get(file));
        MockResponse response = new MockResponse().setBody(new String(bytes));
        mockServer.enqueue(response);
    }

    @Test
    void testGetAllWorkflows() {
        mockServer.enqueue(new MockResponse().setBody("[{\"id\": 123, \"deleted\": false, \"category\": \"Personalization internal\", \"name\": \"test_workflow\"}]"));
        final CtlApiCallsV1 ctlApiCalls = new CtlApiCallsV1(mockServer.url("").toString());
        final List<Workflow> allWorkflows = ctlApiCalls.getAllWorkflows();

        final Workflow expectedWorkflow = new Workflow(123, "test_workflow", "DHP2 Personalization internal", false);
        assertThat(allWorkflows, hasSize(1));
        assertThat(allWorkflows, contains(expectedWorkflow));
    }

    @Test
    void testGetCtlInfo() {
        mockServer.enqueue(new MockResponse().setBody("{\"version\": \"1.2.38\", \"commit\": \"45hj5fjk6\", \"branch\": \"HEAD\"}"));
        final CtlApiCallsV1 ctlApiCalls = new CtlApiCallsV1(mockServer.url("").toString());
        final CtlInfo actual = ctlApiCalls.getCtlInfo();

        final CtlInfo expectedCtlInfo = new CtlInfo("1.2.38", "45hj5fjk6", "HEAD");
        assertEquals(expectedCtlInfo, actual);
    }

    @Test
    void testDeleteWorkflow() {
        mockServer.enqueue(new MockResponse().setBody(""));
        final CtlApiCallsV1 ctlApiCalls = new CtlApiCallsV1(mockServer.url("").toString());
        ctlApiCalls.deleteWorkflow(123);
    }

    @Test
    void testUpdateWfParam() {
        mockServer.enqueue(new MockResponse().setBody("1"));
        final CtlApiCallsV1 ctlApiCalls = new CtlApiCallsV1(mockServer.url("").toString());
        ctlApiCalls.updateWfParam(123, "executorMemory", "10G");
    }

    @Test
    void testPublishStatisticByDummyWf() {
        mockServer.enqueue(new MockResponse().setBody("[{\"id\": 123, \"deleted\": false, \"category\": \"Personalization internal\", \"name\": \"dummy_workflow\"}]"));
        mockServer.enqueue(new MockResponse().setBody("[\"loading_id\", \"11111\"]"));
        mockServer.enqueue(new MockResponse().setResponseCode(201)
                .setBody("The request has been fulfilled and resulted in a new resource being created."));
        mockServer.enqueue(new MockResponse().setBody(""));
        final CtlApiCallsV1 ctlApiCalls = new CtlApiCallsV1(mockServer.url("").toString());
        String profile = "Default";
        ctlApiCalls.createDummyWorkflow(profile);
        ctlApiCalls.runWorkflow(8874, 1);
        ctlApiCalls.publishStatistic(11, profile, 1, "stat_val");
    }

    @Test
    void testCreateDummyWorkflow() {
        mockServer.enqueue(new MockResponse().setBody("[{\"id\": 123, \"deleted\": false, \"category\": \"Personalization internal\", \"name\": \"dummy_workflow_Default\"}]"));
        final CtlApiCallsV1 ctlApiCalls = new CtlApiCallsV1(mockServer.url("").toString());

        final Integer dummyWorkflowId = ctlApiCalls.createDummyWorkflow("Default");
        assertEquals(new Integer(123), dummyWorkflowId);
    }

    @Test
    void testCreateDummyWorkflowWfAlreadyExists() {
        //get wf by name
        mockServer.enqueue(new MockResponse().setBody("[]"));
        //create wf
        mockServer.enqueue(new MockResponse().setBody("{\"id\": 123, \"deleted\": false, \"category\": \"Personalization internal\", \"name\": \"test_workflow_Default\"}"));

        final CtlApiCallsV1 ctlApiCalls = new CtlApiCallsV1(mockServer.url("").toString());

        final Integer dummyWorkflowId = ctlApiCalls.createDummyWorkflow("Default");
        assertEquals(new Integer(123), dummyWorkflowId);
    }

    @Test
    void getCtlBuildDate() {
        final int entityId = 901051004;
        final String profile = "Default";
        mockServer.enqueue(new MockResponse().setBody("[{\"loading_id\":269837,\"profile\":\"Default\",\"entity_id\":901051004,\"stat_id\":5,\"value\":\"2018-06-27\"}," +
                "{\"loading_id\":244878,\"profile\":\"Default\",\"entity_id\":901051004,\"stat_id\":5,\"value\":\"2018-06-28\"}]"));
        final CtlApiCallsV1 ctlApiCalls = new CtlApiCallsV1(mockServer.url("").toString());
        final Optional<LocalDate> date = ctlApiCalls.getStatisticAsLocalDate(entityId, profile, 5);
        assertTrue(date.isPresent());
        assertEquals(date.get(), LocalDate.of(2018, 6, 27));
    }

    @Test
    void testGetLastStatistic() {
        final int entityId = 901051004;
        final int statId = 5;
        mockServer.enqueue(new MockResponse().setBody("{\"name\":\"IBANK123\",\"path\":\"path123\",\"id\":901051004,\"storage\":\"HDFS\",\"parentId\":5}"));
        mockServer.enqueue(new MockResponse().setBody("[{\"loading_id\":269837,\"profile\":\"Default\",\"entity_id\":901051004,\"stat_id\":5,\"value\":\"2018-06-27\"}," +
                "{\"loading_id\":255345,\"profile\":\"clsklrozn\",\"entity_id\":901051004,\"stat_id\":5,\"value\":\"2018-06-26\"}," +
                "{\"loading_id\":244878,\"profile\":\"Default\",\"entity_id\":901051004,\"stat_id\":5,\"value\":\"2018-06-28\"}]"));

        final CtlApiCallsV1 ctlApiCalls = new CtlApiCallsV1(mockServer.url("").toString());
        final Entity entity = ctlApiCalls.getEntity(entityId);
        final Optional<Statistic> statistic = ctlApiCalls.getLastStatistic(entity.id, statId);
        assertTrue(statistic.isPresent());
        assertEquals(entityId, statistic.get().entity_id);
        assertEquals("2018-06-27", statistic.get().value);
    }

    @Test
    void testGetLastStatisticWithProfile() {
        final int entityId = 901051004;
        final int statId = 5;
        final String defaultProfile = "Default";
        mockServer.enqueue(new MockResponse().setBody("{\"name\":\"IBANK123\",\"path\":\"path123\",\"id\":901051004,\"storage\":\"HDFS\",\"parentId\":5}"));
        mockServer.enqueue(new MockResponse().setBody("[{\"loading_id\":269837,\"profile\":\"Default\",\"entity_id\":901051004,\"stat_id\":5,\"value\":\"2018-06-27\"}," +
                "{\"loading_id\":244878,\"profile\":\"Default\",\"entity_id\":901051004,\"stat_id\":5,\"value\":\"2018-06-28\"}]"));

        final CtlApiCallsV1 ctlApiCalls = new CtlApiCallsV1(mockServer.url("").toString());
        final Entity entity = ctlApiCalls.getEntity(entityId);

        // check DefaultProfile
        final Optional<Statistic> statistic = ctlApiCalls.getLastStatisticWithProfile(entity.id, statId, defaultProfile);
        assertTrue(statistic.isPresent());
        assertEquals(entityId, statistic.get().entity_id);
        assertEquals("2018-06-27", statistic.get().value);
    }

    @Test
    void testGetEntity() {
        final int entityId = 123;
        mockServer.enqueue(new MockResponse().setBody("{\"name\":\"IBANK123\",\"path\":\"path123\",\"id\":123,\"storage\":\"HDFS\",\"parentId\":0}"));
        final CtlApiCallsV1 ctlApiCalls = new CtlApiCallsV1(mockServer.url("").toString());
        final Entity entity = ctlApiCalls.getEntity(entityId);
        assertEquals(entityId, entity.id);
    }

    @Test
    void testGetAllStats() {
        final int entityId = 901051004;
        final int statId = 5;
        mockServer.enqueue(new MockResponse().setBody("[{\"loading_id\":269837,\"entity_id\":901051004,\"stat_id\":5,\"value\":\"2018-06-27\"}," +
                "{\"loading_id\":244878,\"entity_id\":901051004,\"stat_id\":5,\"value\":\"2018-06-28\"}]"));
        final CtlApiCallsV1 ctlApiCalls = new CtlApiCallsV1(mockServer.url("").toString());
        final List<Statistic> statistics = ctlApiCalls.getAllStats(entityId, statId);
        assertEquals(2, statistics.size());
        assertEquals(entityId, statistics.get(0).entity_id);
        assertEquals(statId, statistics.get(1).stat_id);
        assertEquals("2018-06-28", statistics.get(1).value);
    }

    @Test
    void getAllStatisticForEntity() {
        final int entityId = 901051004;
        mockServer.enqueue(new MockResponse().setBody("[{\"loading_id\":269837,\"entity_id\":901051004,\"stat_id\":5,\"value\":\"2018-06-27\"}," +
                "{\"loading_id\":244878,\"entity_id\":901051004,\"stat_id\":10,\"value\":\"12345\"}]"));
        final CtlApiCallsV1 ctlApiCalls = new CtlApiCallsV1(mockServer.url("").toString());
        final List<Statistic> statistics = ctlApiCalls.getAllStats(entityId);
        assertEquals(2, statistics.size());
        assertEquals(entityId, statistics.get(0).entity_id);
        assertEquals(5, statistics.get(0).stat_id);
        assertEquals(10, statistics.get(1).stat_id);
        assertEquals("2018-06-27", statistics.get(0).value);
        assertEquals("12345", statistics.get(1).value);
    }

    @Test
    void testGetAllStatsWithProfile() {
        final int entityId = 901051004;
        final int statId = 5;
        final String profile = "Default";
        mockServer.enqueue(new MockResponse().setBody("[{\"loading_id\":269837,\"profile\":\"Default\",\"entity_id\":901051004,\"stat_id\":5,\"value\":\"2018-06-27\"}," +
                "{\"loading_id\":244878,\"profile\":\"clsklrozn\",\"entity_id\":901051004,\"stat_id\":5,\"value\":\"2018-06-28\"}]"));
        final CtlApiCallsV1 ctlApiCalls = new CtlApiCallsV1(mockServer.url("").toString());
        final List<Statistic> statistics = ctlApiCalls.getAllStatsWithProfile(entityId, statId, profile);
        assertEquals(2, statistics.size());
        assertEquals(entityId, statistics.get(0).entity_id);
        assertEquals(statId, statistics.get(1).stat_id);
        assertEquals("2018-06-28", statistics.get(1).value);
    }

    @Test
    void testGetAllCategories() throws IOException {
        enqueueResponseFromFile("src/test/resources/ctl_responses/all_categories_response.json");
        final CtlApiCallsV1 ctlApiCalls = new CtlApiCallsV1(mockServer.url("").toString());
        List<Category> allCategories = ctlApiCalls.getAllCategories();

        assertThat(allCategories, hasSize(2));
        assertEquals(new Category(1369, "DHP2 Personalization internal", false, 0, 1369, ""), allCategories.get(0));
        assertEquals(new Category(50, "Personalization internal integration", false, 0, 50, ""), allCategories.get(1));
    }

    @Test
    void testGetWorkflowsFromCategory() throws IOException {
        final int categoryId = 1369;
        enqueueResponseFromFile("src/test/resources/ctl_responses/workflow_from_category_response.json");
        final CtlApiCallsV1 ctlApiCalls = new CtlApiCallsV1(mockServer.url("").toString());
        List<Workflow> wfs = ctlApiCalls.getWorkflowsFromCategory(categoryId);
        final Workflow expectedWorkflow = new Workflow(123, "test_workflow", "DHP2 Personalization internal", false);

        assertThat(wfs, hasSize(1));
        assertEquals(expectedWorkflow, wfs.get(0));
    }

    @Nested
    @DisplayName("метод stopDummyWorkflowForEntity()")
    class StopDummyWorkflowForEntity {
        @Test
        @DisplayName("завершится со статусом 200; entity удяаляется из Map createdDummies")
        void hasEntity() {
            mockServer.enqueue(new MockResponse().setResponseCode(200));
            final CtlApiCallsV1 ctlApiCalls = new CtlApiCallsV1(mockServer.url("").toString());
            Map<Integer, Integer> entites = new HashMap<>();
            entites.put(1, 887412);
            CtlApiCallsV1.setCreatedDummies(entites);
            ctlApiCalls.stopDummyWorkflowForEntity(1);
            assertFalse(entites.containsKey(1));
        }

        @Test
        @DisplayName("не завершится с ошибкой 400, потому что entity нет в Map createdDummies")
        void hasAnotherEntity() {
            mockServer.enqueue(new MockResponse().setResponseCode(400));
            final CtlApiCallsV1 ctlApiCalls = new CtlApiCallsV1(mockServer.url("").toString());
            Map<Integer, Integer> entites = new HashMap<>();
            entites.put(1, 887412);
            CtlApiCallsV1.setCreatedDummies(entites);
            ctlApiCalls.stopDummyWorkflowForEntity(2);
            assertEquals(1, entites.size());
        }

        @Test
        @DisplayName("не упадёт, если Map createdDummies пустая")
        void noEntity() {
            mockServer.enqueue(new MockResponse().setResponseCode(200));
            final CtlApiCallsV1 ctlApiCalls = new CtlApiCallsV1(mockServer.url("").toString());
            ctlApiCalls.stopDummyWorkflowForEntity(3);
        }

        @Test
        void testGetProfile() throws IOException {
            enqueueResponseFromFile("src/test/resources/ctl_responses/profile_response.json");
            final CtlApiCallsV1 ctlApiCalls = new CtlApiCallsV1(mockServer.url("").toString());
            Profile profileString = ctlApiCalls.getProfile("test");

            System.out.println("HueUri is:" + profileString.hueUri);
            assertNotNull(profileString);
        }

        private void enqueueResponseFromFile(String file) throws IOException {
            byte[] bytes = Files.readAllBytes(Paths.get(file));
            MockResponse response = new MockResponse().setBody(new String(bytes));
            System.out.println(response.toString());
            mockServer.enqueue(response);
        }
    }
}