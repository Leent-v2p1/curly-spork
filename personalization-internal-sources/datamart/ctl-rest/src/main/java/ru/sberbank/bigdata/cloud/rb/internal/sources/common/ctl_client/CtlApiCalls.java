package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client;

import org.json.JSONException;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.dto.*;

import java.io.IOException;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

public interface CtlApiCalls {
    void stopDummyWorkflowForEntity(int entityId) throws JSONException, IOException;
    <T> T getCtlInfo() throws JSONException, IOException;
    <T> T getEntity(int entityId) throws JSONException, IOException;
    LoadingWithStatusResponse getLoading(int loadingId) throws JSONException, IOException;
    void publishStatistic(int statisticId, String profile, int ctlEntityId, String value) throws JSONException, IOException;
    Optional<LocalDate> getStatisticAsLocalDate(int entityId, String profile, int statId) throws JSONException, IOException;
    List<Statistic> getAllStats(int entityId, int statId) throws JSONException, IOException;
    List<Statistic> getAllStats(int entityId) throws JSONException, IOException;
    Optional<Statistic> getLastStatisticWithProfile(int entityId, int statId, String profile) throws JSONException, IOException;
    Profile getProfile(String profile) throws JSONException, IOException;
    Optional<Statistic> getLastStatistic(int entityId, int statId) throws JSONException, IOException;
}
