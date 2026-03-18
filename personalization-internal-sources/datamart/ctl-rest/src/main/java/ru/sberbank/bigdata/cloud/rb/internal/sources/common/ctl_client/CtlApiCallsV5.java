package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import lombok.Getter;
import lombok.SneakyThrows;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.dto.*;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.rest.CtlRestClient;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.rest.RequestResult;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.rest.CtlLoadingStatus;

import java.io.IOException;
import java.lang.reflect.Type;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.*;

public final class CtlApiCallsV5 implements CtlApiCalls {

    private static final Logger log = LoggerFactory.getLogger(CtlApiCallsV5.class);
    private static final String CATEGORY_PERS_INT = "DHP2 Personalization internal";
    private static final String DUMMY_ENGINE = "dummy";
    private static final String DUMMY_WORKFLOW_PREFIX = "dummy_workflow";
    private static Map<Integer, Integer> createdDummies = new HashMap<>();
    private final CtlRestClient restClient;
    Gson gson = new GsonBuilder().create();
    @Getter
    int loadingId;
    @Getter
    int entityId;

    @SneakyThrows
    public CtlApiCallsV5(String ctlUrl) {
        this.restClient = new CtlRestClient(ctlUrl + "/");
    }

    public static void setCreatedDummies(Map<Integer, Integer> createdDummies) {
        CtlApiCallsV5.createdDummies = createdDummies;
    }

    @SneakyThrows
    public String getCtlInfo() {
        RequestResult values = restClient.getCtlInfo();
        JSONObject workflowJson = (new JSONObject(values.getResposeText()));
        final CtlInfo ctlInfo = gson.fromJson(String.valueOf(workflowJson), CtlInfo.class);
        return ctlInfo.getVersion();
    }

    public Integer createDummyWorkflow(String profile) throws JSONException, IOException {
        String wfName = String.join("_", DUMMY_WORKFLOW_PREFIX, profile);
        RequestResult wfByNameCall = restClient.getWfByName(wfName);
        JSONArray workflowsArray = new JSONArray(wfByNameCall.getResposeText());
        int workflowId = -1;
        System.out.println("Length of workflows array: " + workflowsArray.length());
        for (int i = 0; i < workflowsArray.length(); ++i) {
            JSONObject workflow = workflowsArray.getJSONObject(i);
            boolean isDeleted = workflow.getBoolean("deleted");
            if (!isDeleted) {
                workflowId = workflow.getInt("id");
            } else  {
                System.out.println("StStartCread dummy workflow: " + wfName);
                RequestResult newWfByNameCall = restClient.createWf(wfName, "principal", DUMMY_ENGINE, false, CATEGORY_PERS_INT, profile);
                JSONObject workflowJson = (new JSONObject(newWfByNameCall.getResposeText()));
                workflowId = workflowJson.getInt("id");
            }
        }
        System.out.println("Created dummy workflow: " + workflowId);
        return workflowId;
    };

    /**
     * @return loadingId
     */
    public Integer runWorkflow(Integer wfId, int entityId) throws JSONException, IOException {
        RequestResult wfByNameCall = restClient.runWf(wfId);
        JSONArray workflowsArray = new JSONArray(wfByNameCall.getResposeText());
        if (workflowsArray.length() >= 2) {
            int loadingId = workflowsArray.getInt(1);
            System.out.println("Loaded workflows: " + loadingId);
            createdDummies.put(entityId, loadingId);
            return loadingId;
        } else {
            throw new IllegalStateException("Cannot find loadingId in response! for workflow: " + wfId);
        }
    };

    /**
     * Метод останавливает dummy поток по номеру сущности, для которой этот dummy поток публиковал статистику,
     * и удаляет запись из Map createdDummies с ключом entityId.
     * В Map createdDummies хранятся пары entity - loading dummy потока
     *
     * @param entityId сущность, для которой надо оставноить dummy поток
     */
    public void stopDummyWorkflowForEntity(int entityId) throws JSONException, IOException {
        if (createdDummies.containsKey(entityId)) {
            int loadingId = createdDummies.get(entityId);
            this.restClient.stopWf(loadingId);
            createdDummies.remove(entityId);
            log.info("Dummy loading {} for entity {} was stopped", loadingId, entityId);
            return;
        }
        log.info("There's no dummy loading for entity {}", entityId);
    }

    public void publishStatistic(int statisticId, String profile, int ctlEntityId, String value) throws JSONException, IOException {
        int dummyLoadingId = getDummyLoadingId(ctlEntityId, profile);
        System.out.println("dummyLoadingId: " + dummyLoadingId);
        log.info("dummyLoadingId: {}", dummyLoadingId);
        publishStatistic(statisticId, dummyLoadingId, ctlEntityId, value);
    }


    /**
     * Метод создаёт загрузку dummy потока и возвращает loadingId.
     * Если для данной сущности dummy loading поток уже был запущен, то возвращается этот loadingId
     *
     * @param ctlEntityId сущность для которой требуется dummy loading
     * @param profile профиль, в который планируется публиковать статистику для ctlEntityId
     *
     * @return loadingId dummy потока
     */
    public int getDummyLoadingId(int ctlEntityId, String profile) throws JSONException, IOException {
        int dummyLoadingId;
        boolean isWfCreated = createdDummies.containsKey(ctlEntityId);
        if (isWfCreated) {
            System.out.print("dummyLoadingId: " + isWfCreated);
            log.info("Find dummyLoadingId from running workflows");
            dummyLoadingId = createdDummies.get(ctlEntityId);
        } else {
            log.info("Create and run new dummy workflow");
            int dummyWorkflowId = createDummyWorkflow(profile);
            dummyLoadingId = runWorkflow(dummyWorkflowId, ctlEntityId);
        }
        log.info("dummyLoadingId: {}", dummyLoadingId);
        return dummyLoadingId;
    }

    public void publishStatistic(int statisticId, int loadingId, int ctlEntityId, String value) throws JSONException, IOException {
        this.restClient.publishStatistic(loadingId, ctlEntityId, statisticId, value);
    }

    public List<Statistic> getAllStatsWithProfile(int entityId, int statId, String profile) throws JSONException, IOException {
        RequestResult wfByNameCall = restClient.getAllStatisticsForEntityWithProfile(entityId, statId, profile);
        JSONArray workflowsArray = new JSONArray(wfByNameCall.getResposeText());
        Type listType = new TypeToken<List<Statistic>>() {}.getType();
        if (wfByNameCall.getResponseCode() == 200) {
            return gson.fromJson(String.valueOf(workflowsArray), listType);
        } else {
            log.info("No statistics with statId = {} for entity = {} with profile = {}, return empty list", statId, entityId, profile);
            return Collections.emptyList();
        }
    }

    @SneakyThrows
    public List<Statistic> getAllStats(int entityId, int statId) {
        RequestResult values = restClient.getAllStatistics(entityId, Integer.MAX_VALUE, 0);
        JSONArray workflowJson = (new JSONArray(values.getResposeText()));
        Type listType = new TypeToken<List<Statistic>>() {}.getType();
        return gson.fromJson(String.valueOf(workflowJson), listType);
    }

    @Override
    public List<Statistic> getAllStats(int entityId) {
        return null;
    }

    public Optional<Statistic> getLastStatisticWithProfile(int entityId, int statId, String profile) throws JSONException, IOException {
        return getAllStatsWithProfile(entityId, statId, profile).stream().max(Comparator.comparingInt(statistic -> statistic.loading_id));
    }

    public Optional<LocalDate> getStatisticAsLocalDate(int entityId, String profile, int statId) throws JSONException, IOException {
        final Optional<LocalDate> ctlBuildDate = getLastStatisticWithProfile(entityId, statId, profile)
                .map(statistic -> statistic.value)
                .map(LocalDate::parse);
        log.info("Last ctl build date for the entity with id {} and with profile {}, is - {}", entityId, profile, ctlBuildDate);
        return ctlBuildDate;
    }

    public Optional<Statistic> getLastStatistic(int entityId, int statId) {
        return getAllStats(entityId, statId).stream().max(Comparator.comparingInt(statistic -> statistic.loading_id));
    }

    public LoadingWithStatusResponse getLoading(int loadingId) throws JSONException, IOException {
        RequestResult loadingRequestResult = this.restClient.getLoading(loadingId);
        JSONObject loadingValue = new JSONObject(loadingRequestResult.getResposeText());
        Type loadingType = new TypeToken<LoadingWithStatusResponse>() {}.getType();
        return gson.fromJson(String.valueOf(loadingValue), loadingType);
    }

    public Profile getProfile(String profile) throws JSONException, IOException {
        RequestResult loadingRequestResult = this.restClient.getProfile(profile);
        JSONObject profileValue = new JSONObject(loadingRequestResult.getResposeText());
        Type profileType = new TypeToken<Profile>() {}.getType();
        return gson.fromJson(String.valueOf(profileValue), profileType);
    }

    public Entity getEntity(int entityId) throws JSONException, IOException {
        RequestResult loadingRequestResult = this.restClient.getEntityById(entityId);
        JSONObject entityValue = new JSONObject(loadingRequestResult.getResposeText());
        Type entityType = new TypeToken<Entity>() {}.getType();
        return gson.fromJson(String.valueOf(entityValue), entityType);
    }

    private String toUnixTime(LocalDate date) {
        return date == null ? null : String.valueOf(date.atStartOfDay().toInstant(ZoneOffset.of("+3")).toEpochMilli());
    }

    @SneakyThrows
    public void abortLoading() {
        this.restClient.abortLoading(loadingId);
    }

    @SneakyThrows
    public void postStatus(CtlLoadingStatus status, String log) {
        this.restClient.setLoadingStatusWithLog(loadingId, status, log);
    }

    @SneakyThrows
    public void postErrorStatus(String log) {
        this.restClient.setLoadingStatusWithLog(loadingId, CtlLoadingStatus.ERROR, log);
    }

    @SneakyThrows
    public void postSuccessStatus(String log) {
        this.restClient.setLoadingStatusWithLog(loadingId, CtlLoadingStatus.SUCCESS, log);
    }

    @SneakyThrows
    public void postRunningStatus(String log) {
        this.restClient.setLoadingStatusWithLog(loadingId, CtlLoadingStatus.RUNNING, log);
    }

}
