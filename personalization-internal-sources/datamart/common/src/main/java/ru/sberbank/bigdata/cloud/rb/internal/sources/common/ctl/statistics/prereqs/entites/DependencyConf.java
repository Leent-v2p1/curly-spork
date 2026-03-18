package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.prereqs.entites;

import java.util.Arrays;

public class DependencyConf {
    private CommonDependency[] replicaDependency;
    private CommonDependency[] datamartDependency;
    private WorkflowParameters parameters;
    private String eventAwaitStrategy;
    private Cron cron;
    private Locks locks;

    public DependencyConf() {
    }

    public DependencyConf(WorkflowParameters parameters) {
        this.parameters = parameters;
    }

    public DependencyConf(CommonDependency[] replicaDependency,
                          CommonDependency[] datamartDependency,
                          WorkflowParameters parameters,
                          String eventAwaitStrategy,
                          Cron cron) {
        this.replicaDependency = replicaDependency;
        this.datamartDependency = datamartDependency;
        this.parameters = parameters;
        this.eventAwaitStrategy = eventAwaitStrategy;
        this.cron = cron;
    }

    public CommonDependency[] getReplicaDependency() {
        return replicaDependency;
    }

    public void setReplicaDependency(CommonDependency[] replicaDependency) {
        this.replicaDependency = replicaDependency;
    }

    public CommonDependency[] getDatamartDependency() {
        return datamartDependency;
    }

    public void setDatamartDependency(CommonDependency[] datamartDependency) {
        this.datamartDependency = datamartDependency;
    }

    public WorkflowParameters getParameters() {
        return parameters;
    }

    public void setParameters(WorkflowParameters parameters) {
        this.parameters = parameters;
    }

    public String getEventAwaitStrategy() {
        return eventAwaitStrategy;
    }

    public void setEventAwaitStrategy(String eventAwaitStrategy) {
        this.eventAwaitStrategy = eventAwaitStrategy;
    }

    public Cron getCron() {
        return cron;
    }

    public void setCron(Cron cron) {
        this.cron = cron;
    }

    public Locks getLocks() {
        return locks;
    }

    public void setLocks(Locks locks) {
        this.locks = locks;
    }

    @Override
    public String toString() {
        return "DependencyConf{" +
                "replicaDependency=" + Arrays.toString(replicaDependency) +
                ", datamartDependency=" + Arrays.toString(datamartDependency) +
                ", parameters=" + parameters +
                ", eventAwaitStrategy='" + eventAwaitStrategy + '\'' +
                ", cron=" + cron +
                ", locks=" + locks +
                '}';
    }
}
