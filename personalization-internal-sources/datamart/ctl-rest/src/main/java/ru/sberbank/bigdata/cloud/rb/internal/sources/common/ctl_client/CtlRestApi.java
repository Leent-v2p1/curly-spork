package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client;

import okhttp3.ResponseBody;
import retrofit2.Call;
import retrofit2.http.*;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.dto.*;

import java.util.List;

public interface CtlRestApi {
    @POST("wf")
    Call<Workflow> createWf(@Body WorkflowCreate createWf);

    @PUT("wf/{wfId}")
    Call<Workflow> updateWf(@Path("wfId") int wfId, @Body WorkflowUpdate createWf);

    @GET("wf/m")
    Call<List<Workflow>> getAllWorkflows();

    @GET("wf/name/{name}")
    Call<List<Workflow>> getWfByName(@Path("name") String name);

    @GET("entity/{id}")
    Call<Entity> getEntityById(@Path("id") int id);

    @POST("wf/sched/{wfId}")
    Call<String[]> runWf(@Path("wfId") int wfId, @Body EmptyJson emptyJson);

    @POST("param/update")
    Call<Integer> updateWfParam(@Body WorkflowParam workflowParam);

    @PUT("loading/{loadingId}")
    Call<Void> stopWf(@Path("loadingId") int loadingId);

    @DELETE("wf/{wfId}")
    Call<Void> deleteWf(@Path("wfId") int wfId);

    @POST("statval/m")
    Call<ResponseBody> publishStatistic(@Body StatisticUpdate statisticUpdate);

    @GET("statval/all/entity/{entityId}/{limit}/{offset}")
    Call<List<Statistic>> getAllStatistics(@Path("entityId") int entityId,
                                           @Path("limit") int limit,
                                           @Path("offset") int offset);

    @GET("statval/all/{entityId}/{statId}")
    Call<List<Statistic>> getAllStatisticsForEntity(@Path("entityId") int entityId, @Path("statId") int statId);

    @GET("statval/{entityId}/{statId}/{profile}")
    Call<List<Statistic>> getAllStatisticsForEntityWithProfile(@Path("entityId") int entityId,
                                                               @Path("statId") int statId,
                                                               @Path("profile") String profile);

    @POST("loading/filtered")
    Call<List<LoadingResponse>> getLoadings(@Body LoadingFilter loadingFilter);

    @DELETE("loading/{loadingId}")
    Call<Void> cancelLoading(@Path("loadingId") int loadingId);

    @PUT("wf/sched/{wfId}")
    Call<Void> putWorkflowOnSchedule(@Path("wfId") int wfId);

    @GET("loading/{loadingId}")
    Call<LoadingWithStatusResponse> getLoading(@Path("loadingId") int loadingId);

    @POST("loading/filtered")
    Call<List<LoadingResponse>> getFilteredLoadings(@Body DateLoadingFilter loadingFilter);

    @GET("category/m")
    Call<List<Category>> getAllCategories();

    @GET("category/m/{category_id}")
    Call<List<Workflow>> getWorkflowsFromCategory(@Path("category_id") int categoryId);

    @GET("info")
    Call<CtlInfo> getCtlInfo();

    @GET("profile/name/{profile}")
    Call<Profile> getProfile(@Path("profile") String profile);
}
