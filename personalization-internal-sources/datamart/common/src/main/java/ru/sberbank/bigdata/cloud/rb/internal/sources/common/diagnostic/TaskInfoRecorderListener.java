package ru.sberbank.bigdata.cloud.rb.internal.sources.common.diagnostic;

import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.*;
import org.apache.spark.scheduler.cluster.ExecutorInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.lang.System.lineSeparator;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.diagnostic.ListenerType.EXECUTOR;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.diagnostic.ListenerType.TASK;

public class TaskInfoRecorderListener extends SparkListener {

    private static final Logger log = LoggerFactory.getLogger(TaskInfoRecorderListener.class);
    private static TaskInfoRecorderListener instance;

    private final List<TaskVals> taskMetricsData = new ArrayList<>();
    private final List<DynamicMetricsListener<Integer>> executorsListeners = new ArrayList<>();
    private final List<DynamicMetricsListener<Integer>> tasksListeners = new ArrayList<>();
    private Map<Integer, Integer> stageIdtoJobId = new HashMap<>();
    private int currentExecutor;
    private int runningTasks;

    public TaskInfoRecorderListener() {
        if (instance == null) {
            instance = this;
        } else {
            throw new IllegalStateException("Instance of class already created by spark");
        }
    }

    public static TaskInfoRecorderListener instance() {
        return instance;
    }

    public void subscribe(ListenerType type, DynamicMetricsListener<Integer> listener) {
        final Class<? extends DynamicMetricsListener> listenerClass = listener.getClass();

        if (type.equals(EXECUTOR)) {
            executorsListeners.add(listener);
            log.info("subscribed to executorsListeners {}", listenerClass);
            listener.onStateChange(currentExecutor);
        } else if (type.equals(TASK)) {
            tasksListeners.add(listener);
            log.info("subscribed to tasksListeners {}", listenerClass);
            listener.onStateChange(runningTasks);
        }
    }

    List<DynamicMetricsListener<Integer>> getDynamicExecutorsListeners() {
        return executorsListeners;
    }

    int getCurrentExecutor() {
        return currentExecutor;
    }

    void setCurrentCountersToZero() {
        this.currentExecutor = 0;
        this.runningTasks = 0;
    }

    List<DynamicMetricsListener<Integer>> getRunningTasksListeners() {
        return tasksListeners;
    }

    int getRunningTasks() {
        return runningTasks;
    }

    /**
     * collecting stages of the {@code jobStart} to map for future analysis
     *
     * @param jobStart started job
     */
    @Override
    public void onJobStart(SparkListenerJobStart jobStart) {
        JavaConversions.asJavaCollection(jobStart.stageIds())
                .forEach(stageId ->
                        stageIdtoJobId.put((Integer) stageId, jobStart.jobId()));
    }

    @Override
    public void onTaskStart(SparkListenerTaskStart taskStart) {
        runningTasks++;
        tasksListeners.forEach(listener -> listener.onStateChange(runningTasks));
    }

    /**
     * This methods fires at the end of the Task and collects metrics flattened into the {@code taskMetricsData} List
     * All times are in milliseconds, cpu time and shufflewrite are originally in nanosec,
     * thus in the code are divided by 1000 and 1e6 respectively
     */
    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
        runningTasks--;
        tasksListeners.forEach(listener -> listener.onStateChange(runningTasks));

        TaskInfo taskInfo = taskEnd.taskInfo();
        long gettingResultTime = 0L;
        if (taskInfo.gettingResultTime() != 0L) {
            gettingResultTime = (taskInfo.finishTime() - taskInfo.gettingResultTime());
        }
        long duration = (taskInfo.finishTime() - taskInfo.launchTime()) / 1000;
        int jobId = stageIdtoJobId.get(taskEnd.stageId());
        long schedulerDelay = 0L;
        long executorRunTime = 0L;
        TaskMetrics taskMetrics = taskEnd.taskMetrics();
        if (taskMetrics != null) {
            executorRunTime = taskMetrics.executorRunTime();
            schedulerDelay = Math.max(0L, duration - taskMetrics.executorRunTime() - taskMetrics.executorDeserializeTime() -
                    taskMetrics.resultSerializationTime() - gettingResultTime);
        }
        TaskVals currentTask = new TaskVals(jobId,
                taskEnd.stageId(),
                taskInfo.taskId(),
                taskInfo.launchTime(),
                taskInfo.finishTime(),
                duration,
                schedulerDelay,
                taskInfo.executorId(),
                taskInfo.host(),
                taskInfo.taskLocality().toString(),
                taskInfo.speculative(),
                gettingResultTime,
                taskInfo.successful(),
                executorRunTime);
        synchronized (taskMetricsData) {
            taskMetricsData.add(currentTask);
        }
    }

    List<TaskVals> getTaskMetrics() {
        synchronized (taskMetricsData) {
            return new ArrayList<>(taskMetricsData);
        }
    }

    @Override
    public void onStageSubmitted(SparkListenerStageSubmitted stageSubmitted) {
        final StageInfo stageInfo = stageSubmitted.stageInfo();
        String msg = String.format("Stage submitted: %d  AttemptId: %d " +
                        "Parent stages: %s Name: %s number of tasks: %d" + lineSeparator() +
                        "Stack trace: %s" + lineSeparator() +
                        "Stage rdd:" + lineSeparator() + "%s",
                stageInfo.stageId(), stageInfo.attemptNumber(),
                stageInfo.parentIds(), stageInfo.name(), stageInfo.numTasks(),
                stageInfo.details(), stageInfo.rddInfos().mkString(lineSeparator()));
        log.info(msg);
    }

    @Override
    public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
        final StageInfo stageInfo = stageCompleted.stageInfo();
        Long submissionTime = (Long) stageInfo.submissionTime().get();
        Long completionTime = (Long) stageInfo.completionTime().get();
        Long duration = completionTime - submissionTime;
        long durationMinutes = TimeUnit.MILLISECONDS.toMinutes(duration);
        String msg = String.format("Stage completed: %d AttemptId: %d Duration(minutes): %d", stageInfo.stageId(),
                stageInfo.attemptNumber(), durationMinutes);
        log.info(msg);
    }

    @Override
    public void onExecutorAdded(SparkListenerExecutorAdded executorAdded) {
        currentExecutor++;
        executorsListeners.forEach(listener -> listener.onStateChange(currentExecutor));
        final String id = executorAdded.executorId();
        final ExecutorInfo executorInfo = executorAdded.executorInfo();
        final String executorHost = executorInfo.executorHost();
        log.info("Executor added. Current executors: {}, id = {}, {}, {}", currentExecutor, id, executorInfo, executorHost);
    }

    @Override
    public void onExecutorRemoved(SparkListenerExecutorRemoved executorRemoved) {
        currentExecutor--;
        executorsListeners.forEach(listener -> listener.onStateChange(currentExecutor));
        final String reason = executorRemoved.reason();
        final String executorId = executorRemoved.executorId();
        log.info("Executor removed. Current executors: {}, id = {}, reason = {}", currentExecutor, executorId, reason);
    }
}
