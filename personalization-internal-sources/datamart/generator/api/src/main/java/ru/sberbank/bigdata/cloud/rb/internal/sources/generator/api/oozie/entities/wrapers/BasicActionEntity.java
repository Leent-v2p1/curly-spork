package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.wrapers;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.base.WorkflowType;

import java.util.List;

/**
 * Created by sbt-gladyshev-ana on 30.10.2017.
 */
public class BasicActionEntity {
    private final WorkflowType workflowType;
    private final ActionType actionType;
    private final List<ForkEntity> forks;
    private final boolean first;

    public BasicActionEntity(WorkflowType workflowType, ActionType actionType, List<ForkEntity> forks, boolean first) {
        this.workflowType = workflowType;
        this.actionType = actionType;
        this.forks = forks;
        this.first = first;
    }

    public WorkflowType getWorkflowType() {
        return workflowType;
    }

    public ActionType getActionType() {
        return actionType;
    }

    public List<ForkEntity> getForks() {
        return forks;
    }

    public boolean isFirst() {
        return first;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BasicActionEntity that = (BasicActionEntity) o;

        if (first != that.first) {
            return false;
        }
        if (workflowType != that.workflowType) {
            return false;
        }
        if (actionType != that.actionType) {
            return false;
        }
        return forks != null ? forks.equals(that.forks) : that.forks == null;
    }

    @Override
    public int hashCode() {
        int result = workflowType != null ? workflowType.hashCode() : 0;
        result = 31 * result + (actionType != null ? actionType.hashCode() : 0);
        result = 31 * result + (forks != null ? forks.hashCode() : 0);
        result = 31 * result + (first ? 1 : 0);
        return result;
    }
}
