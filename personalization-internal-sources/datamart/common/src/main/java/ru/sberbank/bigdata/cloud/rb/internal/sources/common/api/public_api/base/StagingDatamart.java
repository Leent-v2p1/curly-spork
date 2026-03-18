package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.CtlDefaultStatistics;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticMerger;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticPublisherService;

import java.util.Map;
import java.util.Set;

public abstract class StagingDatamart extends Datamart {

    private static final Logger log = LoggerFactory.getLogger(Datamart.class);

    private CtlDefaultStatistics ctlDefaultStatistics;
    private StatisticPublisherService statisticPublisherService;
    private StatisticMerger statisticMerger;

    @Override
    public void run() {
        log.info("saving staging datamart");
        datamartSaver.save(buildDatamart());
        log.info("publishing statistics for staging datamart");
        processStatistics();
    }

    private void processStatistics() {
        final Map<StatisticId, String> statistics = ctlDefaultStatistics.defaultStatistics();
        log.info("got default statistic, values are: {}", statistics);
        final Map<StatisticId, String> additionalStatistics = extraStatisticAccumulator.getStatistics();
        Set<StatisticId> disabledStatistics =
                disabledStatisticAccumulator.getStatistics().keySet();
        statistics.putAll(additionalStatistics);
        Map<StatisticId, String> filteredStatistics = statisticMerger.merge(statistics, disabledStatistics);
        log.info("filtered statistics, values are: {}", filteredStatistics);
        statisticPublisherService.publishStatistics(filteredStatistics);
    }

    /*Getters and setters*/

    public CtlDefaultStatistics getCtlDefaultStatistics() {
        return ctlDefaultStatistics;
    }

    public void setCtlDefaultStatistics(CtlDefaultStatistics ctlDefaultStatistics) {
        this.ctlDefaultStatistics = ctlDefaultStatistics;
    }

    public StatisticPublisherService getStatisticPublisherService() {
        return statisticPublisherService;
    }

    public void setStatisticPublisherService(StatisticPublisherService statisticPublisherService) {
        this.statisticPublisherService = statisticPublisherService;
    }

    public StatisticMerger getStatisticMerger() {
        return statisticMerger;
    }

    public void setStatisticMerger(StatisticMerger statisticMerger) {
        this.statisticMerger = statisticMerger;
    }
}
