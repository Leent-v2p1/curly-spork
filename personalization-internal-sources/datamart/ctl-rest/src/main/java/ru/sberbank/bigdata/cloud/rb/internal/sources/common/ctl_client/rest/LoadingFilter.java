package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.rest;

import java.util.List;

public class LoadingFilter {
    private Integer workflowId;
    private String xid;
    private List<CtlLoadingState> loadingStates;
    private List<CtlLoadingStatus> loadingStatuses;
    private String startTime;
    private String endTime;
    private Integer numberOfLoadingsToGet;
    private Integer offset;

    LoadingFilter(Integer workflowId, String xid, List<CtlLoadingState> loadingStates, List<CtlLoadingStatus> loadingStatuses, String startTime, String endTime, Integer numberOfLoadingsToGet, Integer offset) {
        this.workflowId = workflowId;
        this.xid = xid;
        this.loadingStates = loadingStates;
        this.loadingStatuses = loadingStatuses;
        this.startTime = startTime;
        this.endTime = endTime;
        this.numberOfLoadingsToGet = numberOfLoadingsToGet;
        this.offset = offset;
    }

    public Integer getWorkflowId() {
        return this.workflowId;
    }

    public String getXid() {
        return this.xid;
    }

    public List<CtlLoadingState> getLoadingStates() {
        return this.loadingStates;
    }

    public List<CtlLoadingStatus> getLoadingStatuses() {
        return this.loadingStatuses;
    }

    public String getStartTime() {
        return this.startTime;
    }

    public String getEndTime() {
        return this.endTime;
    }

    public Integer getNumberOfLoadingsToGet() {
        return this.numberOfLoadingsToGet;
    }

    public Integer getOffset() {
        return this.offset;
    }
}