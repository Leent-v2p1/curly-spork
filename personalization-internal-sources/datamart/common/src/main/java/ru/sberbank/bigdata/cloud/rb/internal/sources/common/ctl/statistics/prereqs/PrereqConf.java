package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.prereqs;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.prereqs.entites.Dependencies;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.prereqs.entites.Locks;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.prereqs.entites.WorkflowParameters;

public class PrereqConf {
    private String name;
    private String eventAwaitStrategy;
    private Dependencies dependencies;
    private WorkflowParameters parameters;
    private Locks locks;

    public PrereqConf(String name,
                      String eventAwaitStrategy,
                      Dependencies dependencies,
                      WorkflowParameters parameters,
                      Locks locks) {
        this.name = name;
        this.eventAwaitStrategy = eventAwaitStrategy;
        this.dependencies = dependencies;
        this.parameters = parameters;
        this.locks = locks;
    }

    public PrereqConf() {
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setEventAwaitStrategy(String eventAwaitStrategy) {
        this.eventAwaitStrategy = eventAwaitStrategy;
    }

    public void setDependencies(Dependencies dependencies) {
        this.dependencies = dependencies;
    }

    public void setParameters(WorkflowParameters parameters) {
        this.parameters = parameters;
    }

    public String getName() {
        return name;
    }

    public String getEventAwaitStrategy() {
        return eventAwaitStrategy;
    }

    public Dependencies getDependencies() {
        return dependencies;
    }

    public WorkflowParameters getParameters() {
        return parameters;
    }

    public Locks getLocks() {
        return locks;
    }

    public void setLocks(Locks locks) {
        this.locks = locks;
    }

    @Override
    public String toString() {
        return "PrereqConf{" +
                "name='" + name + '\'' +
                ", eventAwaitStrategy='" + eventAwaitStrategy + '\'' +
                ", dependencies=" + dependencies +
                ", parameters=" + parameters +
                ", locks=" + locks +
                '}';
    }
}
