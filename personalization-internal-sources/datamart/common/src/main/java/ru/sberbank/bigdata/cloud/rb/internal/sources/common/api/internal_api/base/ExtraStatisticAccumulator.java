package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.base;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.VisibleForTesting;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId;

import java.util.EnumMap;
import java.util.Map;

/**
 * Класс служит для предварительной аггрегации статистик во время построения витрины
 * с помощью метода ExtraStatisticAccumulator#addStatistic.
 */
public class ExtraStatisticAccumulator {

    private final Map<StatisticId, String> statistics = new EnumMap<>(StatisticId.class);

    public void addStatistic(StatisticId statId, String value) {
        statistics.put(statId, value);
    }

    @VisibleForTesting
    public Map<StatisticId, String> getStatistics() {
        return statistics;
    }
}
