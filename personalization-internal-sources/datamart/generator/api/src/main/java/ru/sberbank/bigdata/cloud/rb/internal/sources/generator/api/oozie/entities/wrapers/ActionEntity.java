package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.wrapers;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.base.WorkflowType;

import java.util.List;

/**
 * Created by sbt-gladyshev-ana on 19.09.2017.
 */
public class ActionEntity {
    private final String actionId;
    private final boolean first;
    private final WorkflowType type;
    private final ActionType actionType;
    private final List<ForkEntity> forks;

    public ActionEntity(String actionId, boolean first, WorkflowType type, ActionType actionType, List<ForkEntity> forks) {
        this.actionId = actionId;
        this.first = first;
        this.type = type;
        this.actionType = actionType;
        this.forks = forks;
    }

    public ActionEntity(String actionId, BasicActionEntity basicActionEntity) {
        this.actionId = actionId;
        this.first = basicActionEntity.isFirst();
        this.type = basicActionEntity.getWorkflowType();
        this.actionType = basicActionEntity.getActionType();
        this.forks = basicActionEntity.getForks();
    }

    public String getId() {
        return actionId;
    }

    public WorkflowType getType() {
        return type;
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

    public String getActionName() {
        return getId().split("\\.")[1];
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ActionEntity that = (ActionEntity) o;

        if (first != that.first) {
            return false;
        }
        if (actionId != null ? !actionId.equals(that.actionId) : that.actionId != null) {
            return false;
        }
        if (type != that.type) {
            return false;
        }
        if (actionType != that.actionType) {
            return false;
        }
        return forks != null ? forks.equals(that.forks) : that.forks == null;
    }

    @Override
    public int hashCode() {
        int result = actionId != null ? actionId.hashCode() : 0;
        result = 31 * result + (first ? 1 : 0);
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (actionType != null ? actionType.hashCode() : 0);
        result = 31 * result + (forks != null ? forks.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ActionEntity{" +
                "actionId='" + actionId + '\'' +
                ", first=" + first +
                ", type=" + type +
                ", actionType=" + actionType +
                ", forks=" + forks +
                '}';
    }
}
