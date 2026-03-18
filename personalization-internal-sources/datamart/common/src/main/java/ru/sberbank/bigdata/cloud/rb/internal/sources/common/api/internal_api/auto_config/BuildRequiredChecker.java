package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.auto_config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.dto.Statistic;

import java.time.LocalDate;
import java.util.Optional;

public class BuildRequiredChecker {
    private static final Logger log = LoggerFactory.getLogger(BuildRequiredChecker.class);

    private final boolean skipIfBuiltToday;
    private final Optional<Statistic> lastLoadedStatistic;

    public BuildRequiredChecker(Optional<Statistic> lastLoadedStatistic, boolean skipIfBuiltToday) {
        this.lastLoadedStatistic = lastLoadedStatistic;
        this.skipIfBuiltToday = skipIfBuiltToday;
    }

    public boolean isDatamartBuildingNeeded() {
        if (skipIfBuiltToday && isLastStatisticToday(lastLoadedStatistic)) {
            log.info("There is no need in datamart rebuilding, because it was successfully built earlier today");
            return false;
        }
        log.info("Datamart building is needed");
        return true;
    }

    private boolean isLastStatisticToday(Optional<Statistic> lastLoadedStatistic) {
        boolean isLastLoadedStatisticPresent = lastLoadedStatistic.isPresent();
        log.info("isLastLoadedStatisticPresent = {}", isLastLoadedStatisticPresent);
        if (isLastLoadedStatisticPresent) {
            LocalDate statisticDate = LocalDate.parse(lastLoadedStatistic.get().value.substring(0, 10));
            log.info("Value of 11 statistic : {}", statisticDate);
            return statisticDate.isEqual(LocalDate.now());
        }
        return false;
    }
}
