package ru.sberbank.bigdata.cloud.rb.internal.sources.common.diagnostic;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.base.WorkflowType;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.DatamartNaming;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.VisibleForTesting;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.file.HDFSHelper;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.SparkSQLUtil;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.date.DateHelper.startOfDay;

/**
 * Created by SBT-Yakovlev-AV on 23.10.2017.
 * This is based on using Spark Listeners as data source and collecting metrics in a List
 * The list is then transformed into a Dataset<Row> for analysis
 * <p>
 * You should use {@link #startTimeMeasure() startTimeMeasure} method before start your actual job
 * and {@link #saveReports() saveReports} after job is finished for correct metrics
 * <p>
 * Current metrics:
 * task_durations_sum   - total time of execution of all tasks (in seconds)
 * scheduler_delay      - time between job status schedule -> start (in milliseconds)
 * executor_run_time    - total CPU time (in CPU milliseconds)
 * getting_result_time  - time between (in milliseconds)
 * elapsed_time         - time between job start and finish (in milliseconds)
 * date_build           - moment of time, when metrics tried to save (as timestamp 2018-02-28 15:38:48.332)
 * dir_size_bytes       - size of table directory (in bytes)
 * rows                 - reserve table count(*)
 */
public class TaskMetricsService {

    private static final Logger log = LoggerFactory.getLogger(TaskMetricsService.class);
    @VisibleForTesting
    final Integer executors;
    @VisibleForTesting
    final Double executorMemory;
    @VisibleForTesting
    final Integer executorCoreNum;
    @VisibleForTesting
    final Double executorMemoryOverhead;
    @VisibleForTesting
    final Double driverMemory;
    @VisibleForTesting
    final Double driverMemoryOverhead;
    @VisibleForTesting
    final Integer shufflePartitions;
    private final SparkSession hiveContext;
    private final MetricProviderObserver metricProviderObserver;
    private final TaskInfoRecorderListener taskListener;
    private final FullTableName datamartName;
    private final DatamartNaming naming;
    private final Integer ctlLoadingId;
    private final Integer ctlEntityId;
    private final LocalDate buildDate;
    private final boolean isFirstLoading;
    private WorkflowType workflowType;
    /**
     * Variables used to store the start and end time of the period of interest for the metrics report
     */
    private long beginSnapshot = 0L;
    private long endSnapshot = 0L;

    /**
     * This inserts the custom Spark Listener into the live Spark Context.
     * <p>
     * For correct metrics use {@link #startTimeMeasure() startTimeMeasure} method before start your actual job
     * and {@link #saveReports() saveReports} after job is finished
     */
    public TaskMetricsService(MetricProviderObserver metricProviderObserver,
                              SparkSession hiveContext,
                              FullTableName datamartName,
                              DatamartNaming naming,
                              WorkflowType workflowType,
                              TaskInfoRecorderListener taskListener,
                              Integer ctlLoadingId,
                              Integer ctlEntityId,
                              LocalDate buildDate,
                              boolean isFirstLoading) {
        this.naming = naming;
        this.metricProviderObserver = metricProviderObserver;
        this.datamartName = datamartName;
        this.hiveContext = requireNonNull(hiveContext, "No hive context for saving metrics!");
        this.workflowType = workflowType;
        this.taskListener = taskListener;
        this.ctlLoadingId = ctlLoadingId;
        this.ctlEntityId = ctlEntityId;
        this.buildDate = buildDate;
        this.isFirstLoading = isFirstLoading;
        this.executors = getSparkPropInt(hiveContext, "spark.executor.instances", "-1");
        String executorMemoryStr = getSparkProp(hiveContext, "spark.executor.memory", "-1");
        this.executorMemory = memorySizeFromString(executorMemoryStr);
        this.executorCoreNum = getSparkPropInt(hiveContext, "spark.executor.cores", "-1");
        Integer executorOverheadMb = getSparkPropInt(hiveContext, "spark.yarn.executor.memoryOverhead", "-1");
        this.executorMemoryOverhead = mbToGb(executorOverheadMb);
        String driverMemoryStr = getSparkProp(hiveContext, "spark.driver.memory", "-1");
        this.driverMemory = memorySizeFromString(driverMemoryStr);
        Integer driverMemoryOverheadMb = getSparkPropInt(hiveContext, "spark.yarn.driver.memoryOverhead", "-1");
        this.driverMemoryOverhead = mbToGb(driverMemoryOverheadMb);
        this.shufflePartitions = getSparkPropInt(hiveContext, "spark.sql.shuffle.partitions", "-1");
    }

    private Integer getSparkPropInt(SparkSession hiveContext, String propertyName, String defaultVal) {
        final String prop = getSparkProp(hiveContext, propertyName, defaultVal);
        return Integer.valueOf(prop);
    }

    private String getSparkProp(SparkSession hiveContext, String propertyName, String defaultVal) {
        return hiveContext.sparkContext().conf().get(propertyName, defaultVal);
    }

    public void startTimeMeasure() {
        beginSnapshot = System.currentTimeMillis();
    }

    private void stopTimeMeasure() {
        endSnapshot = System.currentTimeMillis();
    }

    private void publishTaskMetrics() {
        final List<TaskVals> taskList = taskListener.getTaskMetrics();
        AggregatedMetric metrics = getAggregatedMetrics(taskList);
        log.info("Publish aggregated metrics");
        metricProviderObserver.publishAggregatedMetric(metrics);
    }

    @VisibleForTesting
    AggregatedMetric getAggregatedMetrics(List<TaskVals> taskList) {
        TaskVals aggregated = getAggregated(taskList);
        String datamartTable = getDatamartTable();
        return new AggregatedMetric(
                aggregated.getDuration(),
                aggregated.getSchedulerDelay(),
                aggregated.getExecutorRunTime(),
                aggregated.getGettingResultTime(),
                aggregated.getFinishTime() - aggregated.getLaunchTime(),
                startOfDay(LocalDate.now()),
                datamartName.fullTableName(),
                ctlLoadingId,
                ctlEntityId,
                startOfDay(buildDate),
                isFirstLoading,
                executors,
                executorMemory,
                executorCoreNum,
                executorMemoryOverhead,
                driverMemory,
                driverMemoryOverhead,
                shufflePartitions,
                getDatamartSizeInBytes(datamartTable),
                getRowsInDatamart(datamartTable)
        );
    }

    @VisibleForTesting
    protected TaskVals getAggregated(List<TaskVals> taskList) {
        return taskList.stream()
                .filter(task -> {
                    long finishTime = task.getFinishTime();
                    long startTime = task.getLaunchTime();
                    return beginSnapshot <= startTime && finishTime <= endSnapshot;
                })
                .reduce((task1, task2) -> {
                    task1.setDuration(task1.getDuration() + task2.getDuration());
                    task1.setSchedulerDelay(task1.getSchedulerDelay() + task2.getSchedulerDelay());
                    task1.setExecutorRunTime(task1.getExecutorRunTime() + task2.getExecutorRunTime());
                    task1.setGettingResultTime(task1.getGettingResultTime() + task2.getGettingResultTime());
                    task1.setLaunchTime(Math.min(task1.getLaunchTime(), task2.getLaunchTime()));
                    task1.setFinishTime(Math.max(task1.getFinishTime(), task2.getFinishTime()));
                    return task1;
                })
                .orElse(new TaskVals());
    }

    private String getDatamartTable() {
        return WorkflowType.DATAMART.equals(workflowType)
                ? naming.reserveFullTableName()
                : naming.fullTableName();
    }

    private long getRowsInDatamart(String datamartTable) {
        log.info("Counting rows in datamart = {}", datamartTable);
        long rowsInDatamart = -1;
        final boolean exists = SparkSQLUtil.isTableExists(hiveContext, FullTableName.of(datamartTable));
        if (exists) {
            rowsInDatamart = hiveContext.table(datamartTable).count();
            log.info("There're {} rows in datamart = {}", rowsInDatamart, datamartTable);
        } else {
            log.warn("Table {} doesn't exists. Count = {}", datamartTable, rowsInDatamart);
        }
        return rowsInDatamart;
    }

    private long getDatamartSizeInBytes(String datamartTable) {
        Optional<String> pathToReserveDatamart = SparkSQLUtil.tableLocation(hiveContext, datamartTable);
        long datamartSizeInBytes = -1;
        if (pathToReserveDatamart.isPresent()) {
            String path = pathToReserveDatamart.get();
            log.info("Trying to get size of datamart {} directory which hdfs location is {}", datamartTable, path);
            datamartSizeInBytes = HDFSHelper.getDirectorySize(path);
            log.info("Size of hdfs dir {} is {}", path, datamartSizeInBytes);
        } else {
            log.warn("Table {} doesn't exists. Size = {}", datamartTable, datamartSizeInBytes);
        }
        return datamartSizeInBytes;
    }

    /**
     * stops the time count and saves the metrics by calling {@link #publishTaskMetrics() publishTaskMetrics}
     */
    public void saveReports() {
        try {
            stopTimeMeasure();
            log.info("Publish metrics for {}", datamartName);
            publishTaskMetrics();
        } catch (Exception e) {
            log.error("An exception has occurred while publishing metrics. Stacktrace: ", e);
        }
    }

    @VisibleForTesting
    protected Double mbToGb(Integer mb) {
        Double result = mb / 1024.0;
        return Math.round(result * 10) / 10.0;
    }

    @VisibleForTesting
    protected Double memorySizeFromString(String memory) {
        if (memory.equals("-1")) {
            return -1.0;
        }
        String lowerMemory = memory.toLowerCase();
        if (lowerMemory.contains("g")) {
            String gb = lowerMemory.replace("g", "");
            return Double.valueOf(gb);
        } else if (lowerMemory.contains("m")) {
            String mb = lowerMemory.replace("m", "");
            Integer mbInt = Integer.valueOf(mb);
            return mbToGb(mbInt);
        }
        throw new IllegalArgumentException("Unknown memory format " + lowerMemory);
    }
}

