package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Given all statistic and set of disabled statistic, returns only enabled one
 */
public class StatisticMerger {
    private static final Logger log = LoggerFactory.getLogger(StatisticMerger.class);

    public Map<StatisticId, String> merge(Map<StatisticId, String> statistic, Set<StatisticId> disabledStatistics) {
        log.info("input statistics: {}; disabled statistics: {}", statistic, disabledStatistics);
        Map<StatisticId, String> result = statistic.entrySet()
                .stream()
                .filter(stat -> !disabledStatistics.contains(stat.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        log.info("output statistics after filtration of disabled: {}", result);
        return result;
    }
}
