package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.auto_config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.VisibleForTesting;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.Datamart;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.diagnostic.TaskMetricsService;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.DatamartRuntimeException;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.file.HDFSHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class DatamartRunner {
    private static Logger log = LoggerFactory.getLogger(DatamartRunner.class);

    private final TaskMetricsService taskMetricsService;
    private final BuildRequiredChecker buildRequiredChecker;
    private final String statisticTempFile;
    private final List<DatamartEventsListener> datamartEventsListeners = new ArrayList<>();

    public DatamartRunner(TaskMetricsService taskMetricsService,
                          BuildRequiredChecker buildRequiredChecker,
                          String statisticTempFile) {
        this.taskMetricsService = taskMetricsService;
        this.buildRequiredChecker = buildRequiredChecker;
        this.statisticTempFile = statisticTempFile;
    }

    public void subscribe(DatamartEventsListener datamartEventsListener) {
        datamartEventsListeners.add(datamartEventsListener);
    }

    public void run(Datamart datamart) {
        if (!buildRequiredChecker.isDatamartBuildingNeeded()) {
            log.warn("creating empty statistic file {}, because datamart building is not needed", statisticTempFile);
            HDFSHelper.writeFile("", statisticTempFile);
            return;
        }

        taskMetricsService.startTimeMeasure();
        Optional<Throwable> e = runDatamart(datamart);

        if (!e.isPresent()) {
            taskMetricsService.saveReports();
        }
        datamartEventsListeners.forEach(DatamartEventsListener::onDatamartBuildEnd);
        if (e.isPresent()) {
            throw new DatamartRuntimeException(e.get());
        }
    }

    @VisibleForTesting
    public Optional<Throwable> runDatamart(Datamart datamart) {
        Throwable throwable = null;
        try {
            datamart.run();
        } catch (Throwable t) {
            log.error("Exception in datamart.run() occurred {}", t);
            throwable = t;
        }
        return Optional.ofNullable(throwable);
    }
}
