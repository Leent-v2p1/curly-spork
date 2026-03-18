package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client;

import okhttp3.ResponseBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Call;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.dto.*;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.*;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class CtlApiCallsV1 implements CtlApiCalls{
    private static final Logger log = LoggerFactory.getLogger(CtlApiCallsV1.class);

    private static final String CATEGORY_PERS_INT = "DHP2 Personalization internal";
    private static final String DUMMY_ENGINE = "dummy";
    private static final String DUMMY_WORKFLOW_PREFIX = "dummy_workflow";
    private static Map<Integer, Integer> createdDummies = new HashMap<>();
    private final CtlRestApi ctlRestApi;
    private final RetrofitExecutor retrofitExecutor;

    public CtlApiCallsV1(String ctlUrl) {
        retrofitExecutor = new RetrofitExecutor(ctlUrl + Constants.CTL_API_V1 + "/");
        ctlRestApi = retrofitExecutor.createService(CtlRestApi.class);
    }

    public static void setCreatedDummies(Map<Integer, Integer> createdDummies) {
        CtlApiCallsV1.createdDummies = createdDummies;
    }

    public Integer createDummyWorkflow(String profile) {
        String wfName = String.join("_", DUMMY_WORKFLOW_PREFIX, profile);
        final Call<List<Workflow>> wfByNameCall = ctlRestApi.getWfByName(wfName);
        final List<Workflow> wfs = retrofitExecutor.execute(wfByNameCall)
                .ifErrorThrowException();
        return wfs
                .stream()
                .filter(wf -> !wf.deleted)
                .findFirst()
                .orElseGet(() -> {
                    final WorkflowCreate createWf = new WorkflowCreate(wfName, "principal", DUMMY_ENGINE, false, CATEGORY_PERS_INT, profile);
                    final Call<Workflow> call = ctlRestApi.createWf(createWf);
                    final CustomResponse<Workflow> response = retrofitExecutor.execute(call);
                    return response.ifErrorThrowException();
                })
                .id;
    }

    /**
     * @return loadingId
     */
    public Integer runWorkflow(Integer wfId, int entityId) {
        final Call<String[]> call = ctlRestApi.runWf(wfId, new EmptyJson());
        final CustomResponse<String[]> response = retrofitExecutor.execute(call);
        final String[] responseBody = response.ifErrorThrowException();
        if (responseBody.length >= 2) {
            int loadingId = Integer.parseInt(responseBody[1]);
            createdDummies.put(entityId, loadingId);
            return loadingId;
        } else {
            throw new IllegalStateException("Cannot find loadingId in response: " + Arrays.toString(responseBody));
        }
    }

    /**
     * Метод останавливает dummy поток по номеру сущности, для которой этот dummy поток публиковал статистику,
     * и удаляет запись из Map createdDummies с ключом entityId.
     * В Map createdDummies хранятся пары entity - loading dummy потока
     *
     * @param entityId сущность, для которой надо оставноить dummy поток
     */
    public void stopDummyWorkflowForEntity(int entityId) {
        if (createdDummies.containsKey(entityId)) {
            int loadingId = createdDummies.get(entityId);
            final Call<Void> call = ctlRestApi.stopWf(loadingId);
            retrofitExecutor.execute(call).ifErrorThrowException();
            createdDummies.remove(entityId);
            log.info("Dummy loading {} for entity {} was stopped", loadingId, entityId);
            return;
        }
        log.info("There's no dummy loading for entity {}", entityId);
    }

    public List<Workflow> getAllWorkflows() {
        final Call<List<Workflow>> call = ctlRestApi.getAllWorkflows();
        return retrofitExecutor.execute(call).ifErrorThrowException();
    }

    public CtlInfo getCtlInfo() {
        final Call<CtlInfo> call = ctlRestApi.getCtlInfo();
        return retrofitExecutor.execute(call).ifErrorThrowException();
    }

    public void deleteWorkflow(int id) {
        final Call<Void> call = ctlRestApi.deleteWf(id);
        retrofitExecutor.execute(call);
    }

    public void updateWfParam(Integer wfId, String paramName, String newValue) {
        final Call<Integer> call = ctlRestApi.updateWfParam(new WorkflowParam(wfId, paramName, newValue));
        retrofitExecutor.execute(call);
    }

    public void publishStatistic(int statisticId, String profile, int ctlEntityId, String value) {
        int dummyLoadingId = getDummyLoadingId(ctlEntityId, profile);
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
    public int getDummyLoadingId(int ctlEntityId, String profile) {
        int dummyLoadingId;
        boolean isWfCreated = createdDummies.containsKey(ctlEntityId);
        if (isWfCreated) {
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

    public void publishStatistic(int statisticId, int loadingId, int ctlEntityId, String value) {
        final StatisticUpdate statisticUpdate = new StatisticUpdate(loadingId, ctlEntityId, statisticId, singletonList(value));
        final Call<ResponseBody> call = ctlRestApi.publishStatistic(statisticUpdate);
        retrofitExecutor.execute(call).ifErrorThrowException();
    }

    public List<Statistic> getAllStatsWithProfile(int entityId, int statId, String profile) {
        final Call<List<Statistic>> call = ctlRestApi.getAllStatisticsForEntityWithProfile(entityId, statId, profile);
        CustomResponse<List<Statistic>> execute = retrofitExecutor.execute(call);
        if (execute.isSuccess()) {
            return execute.response();
        } else {
            log.info("No statistics with statId = {} for entity = {} with profile = {}, return empty list", statId, entityId, profile);
            return Collections.emptyList();
        }
    }

    public List<Statistic> getAllStats(int entityId) {
        final Call<List<Statistic>> call = ctlRestApi.getAllStatistics(entityId, Integer.MAX_VALUE, 0);
        return retrofitExecutor.execute(call).ifErrorThrowException();
    }

    public List<Statistic> getAllStats(int entityId, int statId) {
        final Call<List<Statistic>> call = ctlRestApi.getAllStatisticsForEntity(entityId, statId);
        CustomResponse<List<Statistic>> execute = retrofitExecutor.execute(call);
        if (execute.isSuccess()) {
            return execute.response();
        } else {
            log.info("No statistics with statId = {} for entity = {}, return empty list", statId, entityId);
            return Collections.emptyList();
        }
    }

    public Entity getEntity(int entityId) {
        final Call<Entity> call = ctlRestApi.getEntityById(entityId);
        return retrofitExecutor.execute(call).ifErrorThrowException();
    }

    public Optional<Statistic> getLastStatisticWithProfile(int entityId, int statId, String profile) {
        return getAllStatsWithProfile(entityId, statId, profile).stream().max(Comparator.comparingInt(statistic -> statistic.loading_id));
    }

    public Optional<Statistic> getLastStatistic(int entityId, int statId) {
        return getAllStats(entityId, statId).stream().max(Comparator.comparingInt(statistic -> statistic.loading_id));
    }

    public Optional<LocalDate> getStatisticAsLocalDate(int entityId, String profile, int statId) {
        final Optional<LocalDate> ctlBuildDate = getLastStatisticWithProfile(entityId, statId, profile)
                .map(statistic -> statistic.value)
                .map(LocalDate::parse);
        log.info("Last ctl build date for the entity with id {} and with profile {}, is - {}", entityId, profile, ctlBuildDate);
        return ctlBuildDate;
    }

    public LoadingWithStatusResponse getLoading(int loadingId) {
        return retrofitExecutor.execute(ctlRestApi.getLoading(loadingId)).ifErrorThrowException();
    }

    /**
     * @param endDate if null method will return all loadings without filtration
     * Get loading that ended before 'endDate'
     */
    public List<LoadingResponse> getLoadingEndedInTime(LocalDate endDate, int wfId) {
        final String unixTimeTo = toUnixTime(endDate);
        final DateLoadingFilter dateLoadingFilter = new DateLoadingFilter(wfId, unixTimeTo, singletonList("SUCCESS"));
        final Call<List<LoadingResponse>> call = ctlRestApi.getFilteredLoadings(dateLoadingFilter);
        return retrofitExecutor.execute(call).ifErrorThrowException();
    }

    public List<LoadingResponse> getActiveLoadings(int wfId) {
        LoadingFilter activeFilter = new LoadingFilter(wfId, singletonList("ACTIVE"), asList("ERROR", "TIME-WAIT", "EVENT-WAIT", "RUNNING"));
        final Call<List<LoadingResponse>> call = ctlRestApi.getLoadings(activeFilter);
        return retrofitExecutor.execute(call).ifErrorThrowException();
    }

    public List<Category> getAllCategories() {
        Call<List<Category>> call = ctlRestApi.getAllCategories();
        return retrofitExecutor.execute(call).ifErrorThrowException();
    }

    public List<Workflow> getWorkflowsFromCategory(int categoryId) {
        Call<List<Workflow>> call = ctlRestApi.getWorkflowsFromCategory(categoryId);
        return retrofitExecutor.execute(call).ifErrorThrowException();
    }

    public Profile getProfile(String profile) {
        return retrofitExecutor.execute(ctlRestApi.getProfile(profile)).ifErrorThrowException();
    }

    private String toUnixTime(LocalDate date) {
        return date == null ? null : String.valueOf(date.atStartOfDay().toInstant(ZoneOffset.of("+3")).toEpochMilli());
    }
}
