package ru.sberbank.bigdata.cloud.rb.internal.sources.common.diagnostic;

import org.joda.time.format.DateTimeFormatter;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;

public class AggregatedMetric {

    private Long taskDurationsSum;
    private Long schedulerDelay;
    private Long executorRunTime;
    private Long gettingResultTime;
    private Long elapsedTime;
    private Timestamp dateBuild;
    private String datamartName;
    private Integer ctlLoadingId;
    private Integer ctlEntityId;
    private Timestamp businessDate;
    private Boolean isFirstLoading;
    private Integer executorsAmount;
    private Double executorMemory;
    private Integer executorCoreNum;
    private Double executorMemoryOverhead;
    private Double driverMemory;
    private Double driverMemoryOverhead;
    private Integer shufflePartitions;
    private Long dirSizeBytes;
    private Long rowsCount;
    private String timestampOfMetric;

    public AggregatedMetric(Long taskDurationsSum,
                            Long schedulerDelay,
                            Long executorRunTime,
                            Long gettingResultTime,
                            Long elapsedTime,
                            Timestamp dateBuild,
                            String datamartName,
                            Integer ctlLoadingId,
                            Integer ctlEntityId,
                            Timestamp businessDate,
                            Boolean isFirstLoading,
                            Integer executorsAmount,
                            Double executorMemory,
                            Integer executorCoreNum,
                            Double executorMemoryOverhead,
                            Double driverMemory,
                            Double driverMemoryOverhead,
                            Integer shufflePartitions, Long dirSizeBytes, Long rowsCount) {
        this.taskDurationsSum = taskDurationsSum;
        this.schedulerDelay = schedulerDelay;
        this.executorRunTime = executorRunTime;
        this.gettingResultTime = gettingResultTime;
        this.elapsedTime = elapsedTime;
        this.dateBuild = dateBuild;
        this.datamartName = datamartName;
        this.ctlLoadingId = ctlLoadingId;
        this.ctlEntityId = ctlEntityId;
        this.businessDate = businessDate;
        this.isFirstLoading = isFirstLoading;
        this.executorsAmount = executorsAmount;
        this.executorMemory = executorMemory;
        this.executorCoreNum = executorCoreNum;
        this.executorMemoryOverhead = executorMemoryOverhead;
        this.driverMemory = driverMemory;
        this.driverMemoryOverhead = driverMemoryOverhead;
        this.shufflePartitions = shufflePartitions;
        this.dirSizeBytes = dirSizeBytes;
        this.rowsCount = rowsCount;
    }

    public AggregatedMetric() {
    }

    public Long getTaskDurationsSum() {
        return taskDurationsSum;
    }

    public Long getSchedulerDelay() {
        return schedulerDelay;
    }

    public Long getExecutorRunTime() {
        return executorRunTime;
    }

    public Long getGettingResultTime() {
        return gettingResultTime;
    }

    public Long getElapsedTime() {
        return elapsedTime;
    }

    public Timestamp getDateBuild() {
        return dateBuild;
    }

    public String getDatamartName() {
        return datamartName;
    }

    public Integer getCtlLoadingId() {
        return ctlLoadingId;
    }

    public Integer getCtlEntityId() {
        return ctlEntityId;
    }

    public Timestamp getBusinessDate() {
        return businessDate;
    }

    public Boolean getFirstLoading() {
        return isFirstLoading;
    }

    public Integer getExecutorsAmount() {
        return executorsAmount;
    }

    public Double getExecutorMemory() {
        return executorMemory;
    }

    public Integer getExecutorCoreNum() {
        return executorCoreNum;
    }

    public Double getExecutorMemoryOverhead() {
        return executorMemoryOverhead;
    }

    public Double getDriverMemory() {
        return driverMemory;
    }

    public Double getDriverMemoryOverhead() {
        return driverMemoryOverhead;
    }

    public Integer getShufflePartitions() {
        return shufflePartitions;
    }

    public Long getDirSizeBytes() {
        return dirSizeBytes;
    }

    public Long getRowsCount() {
        return rowsCount;
    }

    @Override
    public String toString() {
        return "AggregatedMetric{" +
                "taskDurationsSum=" + taskDurationsSum +
                ", schedulerDelay=" + schedulerDelay +
                ", executorRunTime=" + executorRunTime +
                ", gettingResultTime=" + gettingResultTime +
                ", elapsedTime=" + elapsedTime +
                ", dateBuild=" + dateBuild +
                ", datamartName='" + datamartName + '\'' +
                ", ctlLoadingId=" + ctlLoadingId +
                ", ctlEntityId=" + ctlEntityId +
                ", businessDate=" + businessDate +
                ", isFirstLoading=" + isFirstLoading +
                ", executorsAmount=" + executorsAmount +
                ", executorMemory=" + executorMemory +
                ", executorCoreNum=" + executorCoreNum +
                ", executorMemoryOverhead=" + executorMemoryOverhead +
                ", driverMemory=" + driverMemory +
                ", driverMemoryOverhead=" + driverMemoryOverhead +
                ", shufflePartitions=" + shufflePartitions +
                ", dirSizeBytes=" + dirSizeBytes +
                ", rowsCount=" + rowsCount +
                '}';
    }

    public String toStringNow() {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        timestampOfMetric = dateFormat.format(System.currentTimeMillis());
        //timestampOfMetric = String.valueOf(LocalDateTime.now());
        return timestampOfMetric + " AggregatedMetric{" +
                "taskDurationsSum=" + taskDurationsSum +
                ", schedulerDelay=" + schedulerDelay +
                ", executorRunTime=" + executorRunTime +
                ", gettingResultTime=" + gettingResultTime +
                ", elapsedTime=" + elapsedTime +
                ", dateBuild=" + dateBuild +
                ", datamartName='" + datamartName + '\'' +
                ", ctlLoadingId=" + ctlLoadingId +
                ", ctlEntityId=" + ctlEntityId +
                ", businessDate=" + businessDate +
                ", isFirstLoading=" + isFirstLoading +
                ", executorsAmount=" + executorsAmount +
                ", executorMemory=" + executorMemory +
                ", executorCoreNum=" + executorCoreNum +
                ", executorMemoryOverhead=" + executorMemoryOverhead +
                ", driverMemory=" + driverMemory +
                ", driverMemoryOverhead=" + driverMemoryOverhead +
                ", shufflePartitions=" + shufflePartitions +
                ", dirSizeBytes=" + dirSizeBytes +
                ", rowsCount=" + rowsCount +
                '}';
    }

    public String toCsvFormat() {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        timestampOfMetric = dateFormat.format(System.currentTimeMillis());
        //timestampOfMetric = String.valueOf(LocalDateTime.now());
        return timestampOfMetric +
                " datamartName='" + datamartName + '\'' +
                ", ctlEntityId=" + ctlEntityId +
                ", ctlLoadingId=" + ctlLoadingId +
                ", taskDurationsSum=" + taskDurationsSum +
                ", isFirstLoading=" + isFirstLoading +
                ", dirSizeBytes=" + dirSizeBytes +
                ", rowsCount=" + rowsCount +
                ", schedulerDelay=" + schedulerDelay +
                ", executorRunTime=" + executorRunTime +
                ", gettingResultTime=" + gettingResultTime +
                ", elapsedTime=" + elapsedTime +
                ", dateBuild=" + dateBuild +
                ", businessDate=" + businessDate +
                ", executorsAmount=" + executorsAmount +
                ", executorMemory=" + executorMemory +
                ", executorCoreNum=" + executorCoreNum +
                ", executorMemoryOverhead=" + executorMemoryOverhead +
                ", driverMemory=" + driverMemory +
                ", driverMemoryOverhead=" + driverMemoryOverhead +
                ", shufflePartitions=" + shufflePartitions;
    }
}