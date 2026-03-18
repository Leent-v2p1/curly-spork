package ru.sberbank.bigdata.cloud.rb.internal.sources.common.diagnostic;

/**
 * getters and setters are used as in JavaBean for
 * hiveContext.createDataFrame(listenerTask.taskMetricsData, TaskVals.class)
 */
@SuppressWarnings("unused")
public class TaskVals {
    private int jobId;
    private int stageId;
    private long index;
    private long launchTime;
    private long finishTime;
    private long duration;
    private long schedulerDelay;
    private String executorId;
    private String host;
    private String taskLocality;
    private boolean speculative;
    private long gettingResultTime;
    private boolean successful;
    private long executorRunTime;

    @SuppressWarnings("unused")
    public TaskVals() {
    }

    TaskVals(int jobId,
             int stageId,
             long index,
             long launchTime,
             long finishTime,
             long duration,
             long schedulerDelay,
             String executorId,
             String host,
             String taskLocality,
             boolean speculative,
             long gettingResultTime,
             boolean successful,
             long executorRunTime) {
        this.jobId = jobId;
        this.stageId = stageId;
        this.index = index;
        this.launchTime = launchTime;
        this.finishTime = finishTime;
        this.duration = duration;
        this.schedulerDelay = schedulerDelay;
        this.executorId = executorId;
        this.host = host;
        this.taskLocality = taskLocality;
        this.speculative = speculative;
        this.gettingResultTime = gettingResultTime;
        this.successful = successful;
        this.executorRunTime = executorRunTime;
    }

    public int getJobId() {
        return jobId;
    }

    public void setJobId(int jobId) {
        this.jobId = jobId;
    }

    public int getStageId() {
        return stageId;
    }

    public void setStageId(int stageId) {
        this.stageId = stageId;
    }

    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    public long getLaunchTime() {
        return launchTime;
    }

    public void setLaunchTime(long launchTime) {
        this.launchTime = launchTime;
    }

    public long getFinishTime() {
        return finishTime;
    }

    public void setFinishTime(long finishTime) {
        this.finishTime = finishTime;
    }

    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }

    public long getSchedulerDelay() {
        return schedulerDelay;
    }

    public void setSchedulerDelay(long schedulerDelay) {
        this.schedulerDelay = schedulerDelay;
    }

    public String getExecutorId() {
        return executorId;
    }

    public void setExecutorId(String executorId) {
        this.executorId = executorId;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getTaskLocality() {
        return taskLocality;
    }

    public void setTaskLocality(String taskLocality) {
        this.taskLocality = taskLocality;
    }

    public boolean isSpeculative() {
        return speculative;
    }

    public void setSpeculative(boolean speculative) {
        this.speculative = speculative;
    }

    public long getGettingResultTime() {
        return gettingResultTime;
    }

    public void setGettingResultTime(long gettingResultTime) {
        this.gettingResultTime = gettingResultTime;
    }

    public boolean isSuccessful() {
        return successful;
    }

    public void setSuccessful(boolean successful) {
        this.successful = successful;
    }

    public long getExecutorRunTime() {
        return executorRunTime;
    }

    public void setExecutorRunTime(long executorRunTime) {
        this.executorRunTime = executorRunTime;
    }

    @Override
    public String toString() {
        return "TaskVals{" +
                "jobId=" + jobId +
                ", duration=" + duration + '\'' +
                '}';
    }
}
