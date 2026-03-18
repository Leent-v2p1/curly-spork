package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.file.HDFSHelper;

/**
 * StatisticToFileWriter writes a statistic to file <code>pathToStatistic</code> and statistics id to file <code>pathToDisabledStatisticFile</code>.
 * File with statistics accumulates all statistics written with StatisticToFileWriter#writeStatistic method.
 * File with statistics id accumulates all statistics id that should not be published written with StatisticToFileWriter#writeStatId method.
 * <p>
 * Statistic File format:
 * code=value with spaces
 * <p>
 * Statistic id File format:
 * 2
 * 10
 */
public class StatisticToFileWriter {

    private static final Logger log = LoggerFactory.getLogger(StatisticToFileWriter.class);
    private final String pathToStatistic;
    private final String pathToDisabledStatistic;

    public StatisticToFileWriter(String pathToStatistic, String pathToDisabledStatistic) {
        this.pathToStatistic = pathToStatistic;
        this.pathToDisabledStatistic = pathToDisabledStatistic;
    }

    public void writeStatistic(StatisticId statisticId, String value) {
        String newStatistic = statisticId.getCode() + "=" + value;
        HDFSHelper.append(newStatistic, pathToStatistic);
        log.info("Written statistic {} with value {} to file {}", statisticId, value, pathToStatistic);
    }

    public void writeDisabledStatistic(StatisticId statisticId) {
        String statId = String.valueOf(statisticId.getCode());
        HDFSHelper.append(statId, pathToDisabledStatistic);
        log.info("Written statistic {} to file {}", statisticId, pathToDisabledStatistic);
    }
}
