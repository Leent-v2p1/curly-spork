package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics;

import lombok.SneakyThrows;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.CtlApiCalls;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.dto.Entity;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class StatisticPublisherService {

    private static final Logger log = LoggerFactory.getLogger(StatisticPublisherService.class);

    private final CtlApiCalls ctl;
    private final Optional<Integer> entityIdOptional;
    private final String profile;
    private final Environment env;

    public StatisticPublisherService(CtlApiCalls ctl,
                                     Optional<Integer> entityIdOptional,
                                     Environment env,
                                     String profile) {
        this.ctl = ctl;
        this.entityIdOptional = entityIdOptional;
        this.env = env;
        this.profile = profile;
    }

    @SneakyThrows
    public void publishStatistics(Map<StatisticId, String> statistics) {
        if (statistics.isEmpty()) {
            log.info("There are no statistics to publish");
            return;
        }

        final Integer entityId = entityIdOptional.orElseThrow(() ->
                new IllegalArgumentException("entityId is absent. Can't publish ctl statistic without entityId"));
        validateEntityId(entityId);

        if (env.isTestEnvironment()) {
            log.info("Statistic won't be published because current environment = '{}'", env);
            return;
        }

        for (Map.Entry<StatisticId, String> statistic : statistics.entrySet()) {
            final int statId = statistic.getKey().getCode();
            final String value = statistic.getValue();

            publishStatistic(statId, entityId, value);
        }
        ctl.stopDummyWorkflowForEntity(entityId);
    }

    @SneakyThrows
    private void publishStatistic(int statisticId, int ctlEntityId, String value) {
        log.info("Publishing statistic '{}' with value '{}' and profile '{}' for entity {}", statisticId, value, profile, ctlEntityId);
        ctl.publishStatistic(statisticId, profile, ctlEntityId, value);
    }

    private void validateEntityId(Integer entityId) throws JSONException, IOException {
        Entity entity = ctl.getEntity(entityId);
        Objects.requireNonNull(entity, "Entity not found. id = " + entityId);
    }
}
