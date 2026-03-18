package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.EnumMap;
import java.util.Map;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId.*;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.date.DateHelper.ISO_DATE_TIME_WITHOUT_SECONDS;

/**
 * Default statistics with values that would be used in StagingDatamart.run(), CatalogUploadJob.run() and reserving (for regular datamarts)
 */
public class CtlDefaultStatistics {

    private static final Logger log = LoggerFactory.getLogger(CtlDefaultStatistics.class);

    private final boolean shouldUpdateMonthStat;
    private final LocalDate buildDate;
    private final LocalDateTime currentTime;
    private final int loadingId;
    private final String loadingType;

    public CtlDefaultStatistics(boolean shouldUpdateMonthStat,
                                LocalDate buildDate,
                                LocalDateTime currentTime,
                                int loadingId,
                                String loadingType) {
        this.shouldUpdateMonthStat = shouldUpdateMonthStat;
        this.buildDate = buildDate;
        this.currentTime = currentTime;
        this.loadingId = loadingId;
        this.loadingType = loadingType;
    }

    public Map<StatisticId, String> defaultStatistics() {
        Map<StatisticId, String> defaultStatistics = new EnumMap<>(StatisticId.class);
        defaultStatistics.put(CHANGE_STAT_ID, CHANGE_STAT_VALUE);
        defaultStatistics.put(LAST_LOADED_STAT_ID, currentTime.format(ISO_DATE_TIME_WITHOUT_SECONDS));
        defaultStatistics.put(BUSINESS_DATE_STAT_ID, buildDate.minusDays(1).format(ISO_LOCAL_DATE));
        defaultStatistics.put(CSN, String.valueOf(loadingId));
        defaultStatistics.put(LOADING_TYPE, loadingType);

        if (shouldUpdateMonthStat) {
            log.info("Flag shouldUpdateMonthStat is true. add {} to default statistics", MONTH_WF_SCHEDULE);
            defaultStatistics.put(MONTH_WF_SCHEDULE, buildDate.toString());
        }
        return defaultStatistics;
    }
}
