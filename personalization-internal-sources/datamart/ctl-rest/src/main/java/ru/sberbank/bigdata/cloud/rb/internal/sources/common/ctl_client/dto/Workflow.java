package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.dto;

import java.util.List;
import java.util.Objects;

public class Workflow {
    public Integer id;
    public String name;
    public String engine;
    public String eventAwaitStrategy;
    public boolean scheduled;
    public String category;
    public String type;
    public boolean deleted;
    private List<WorkflowParam> param;

    public Workflow() {
    }

    public Workflow(Integer id, String name, String category, boolean deleted) {
        this.id = id;
        this.name = name;
        this.category = category;
        this.deleted = deleted;
    }

    @Override
    public String toString() {
        return "Workflow{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", engine='" + engine + '\'' +
                ", eventAwaitStrategy='" + eventAwaitStrategy + '\'' +
                ", scheduled=" + scheduled +
                ", category='" + category + '\'' +
                ", type='" + type + '\'' +
                ", deleted=" + deleted +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Workflow workflow = (Workflow) o;
        return deleted == workflow.deleted &&
                Objects.equals(id, workflow.id) &&
                Objects.equals(name, workflow.name) &&
                Objects.equals(engine, workflow.engine) &&
                Objects.equals(eventAwaitStrategy, workflow.eventAwaitStrategy) &&
                Objects.equals(scheduled, workflow.scheduled) &&
                Objects.equals(category, workflow.category) &&
                Objects.equals(type, workflow.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, engine, eventAwaitStrategy, scheduled, category, type, deleted);
    }

    public List<WorkflowParam> getParam() {
        return param;
    }

    public void setParam(List<WorkflowParam> param) {
        this.param = param;
    }
}
