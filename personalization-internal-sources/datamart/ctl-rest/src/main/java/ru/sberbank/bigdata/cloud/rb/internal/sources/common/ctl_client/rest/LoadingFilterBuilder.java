package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.rest;

import java.util.List;

public class LoadingFilterBuilder {
    private Integer workflowId = null;
    private String xid = null;
    private List<CtlLoadingState> loadingStates = null;
    private List<CtlLoadingStatus> loadingStatuses = null;
    private String startTime = null;
    private String endTime = null;
    private Integer numberOfLoadingsToGet = null;
    private Integer offset = null;

    public LoadingFilterBuilder() {
    }

    public LoadingFilterBuilder setWorkflowId(Integer workflowId) {
        this.workflowId = workflowId;
        return this;
    }

    public LoadingFilterBuilder setXid(String xid) {
        this.xid = xid;
        return this;
    }

    public LoadingFilterBuilder setLoadingStates(List<CtlLoadingState> loadingStates) {
        this.loadingStates = loadingStates;
        return this;
    }

    public LoadingFilterBuilder setLoadingStatuses(List<CtlLoadingStatus> loadingStatuses) {
        this.loadingStatuses = loadingStatuses;
        return this;
    }

    public LoadingFilterBuilder setStartTime(String startTime) {
        this.startTime = startTime;
        return this;
    }

    public LoadingFilterBuilder setEndTime(String endTime) {
        this.endTime = endTime;
        return this;
    }

    public LoadingFilterBuilder setNumberOfLoadingsToGet(Integer numberOfLoadingsToGet) {
        this.numberOfLoadingsToGet = numberOfLoadingsToGet;
        return this;
    }

    public LoadingFilterBuilder setOffset(Integer offset) {
        this.offset = offset;
        return this;
    }

    public LoadingFilter createLoadingFilter() {
        return new LoadingFilter(this.workflowId, this.xid, this.loadingStates, this.loadingStatuses, this.startTime, this.endTime, this.numberOfLoadingsToGet, this.offset);
    }
}