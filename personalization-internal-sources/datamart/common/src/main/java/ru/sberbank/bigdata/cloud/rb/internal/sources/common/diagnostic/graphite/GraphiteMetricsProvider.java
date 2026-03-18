package ru.sberbank.bigdata.cloud.rb.internal.sources.common.diagnostic.graphite;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.Datamart;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.diagnostic.AggregatedMetric;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.diagnostic.MetricListener;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.DatamartRuntimeException;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.file.HDFSHelper;

import java.io.IOException;

/**
 * Класс публикует метрики по построенной витрине после ее построения (1 раз)
 */
public class GraphiteMetricsProvider implements MetricListener {

    private static final Logger log = LoggerFactory.getLogger(GraphiteMetricsProvider.class);
    private final Environment env;
    private ScheduledReporter reporter;
    private MetricRegistry registry;
    private String pathToMetrics = "/oozie-app/wf/custom/rb/production/stats/all_datamarts.txt";

    public GraphiteMetricsProvider(ScheduledReporter reporter, MetricRegistry registry, Environment env) {
        this.reporter = reporter;
        this.registry = registry;
        this.env = env;
    }

    @Override
    public void onAggregatedMetric(AggregatedMetric metric) {
        log.info("sending metric to graphite {}", metric);

        //to Audit Logger
        Datamart.stgAppLogDm(metric.getCtlLoadingId(), metric.getDirSizeBytes(), metric.getRowsCount());

        try {
            pathToMetrics = "/oozie-app/wf/custom/rb/production/stats/" + metric.getDatamartName() + "_stat.txt";
            HDFSHelper.append(metric.toCsvFormat(), pathToMetrics);
            log.info("Save metrics to {}", pathToMetrics);
        } catch (Exception e) {
            log.error("Can't create file : {}", pathToMetrics);
        }
        if (env.isTestEnvironment()) {
            log.info("Current environment is {}}. Class will not publish metric.", env);
            return;
        }

        registry.gauge("task_durations_sum", () -> metric::getTaskDurationsSum);
        registry.gauge("scheduler_delay", () -> metric::getSchedulerDelay);
        registry.gauge("executor_run_time", () -> metric::getExecutorRunTime);
        registry.gauge("getting_result_time", () -> metric::getGettingResultTime);
        registry.gauge("elapsed_time", () -> metric::getElapsedTime);
        registry.gauge("executors_amount", () -> metric::getExecutorsAmount);
        registry.gauge("executor_memory", () -> () -> (metric.getExecutorMemory().intValue()));
        registry.gauge("executor_core_num", () -> metric::getExecutorCoreNum);
        registry.gauge("executor_memory_overhead", () -> () -> (metric.getExecutorMemoryOverhead().intValue()));
        registry.gauge("driver_memory", () -> () -> (metric.getDriverMemory().intValue()));
        registry.gauge("driver_memory_overhead", () -> () -> (metric.getDriverMemoryOverhead().intValue()));
        registry.gauge("shuffle_partitions", () -> metric::getShufflePartitions);
        registry.gauge("dir_size_bytes", () -> metric::getDirSizeBytes);
        registry.gauge("rows_count", () -> metric::getRowsCount);
        registry.gauge("is_first_loading", () -> () -> (mapBoolToInt(metric.getFirstLoading())));

        reporter.report();
        log.info("finished sending metric to graphite ");
    }

    private int mapBoolToInt(Boolean boolValue) {
        return Boolean.TRUE.equals(boolValue) ? 1 : 0;
    }
}
