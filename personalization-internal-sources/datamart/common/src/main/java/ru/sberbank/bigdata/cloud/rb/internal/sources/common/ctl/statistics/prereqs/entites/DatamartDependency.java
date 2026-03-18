package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.prereqs.entites;

import java.util.Arrays;

public class DatamartDependency {
    private Integer statId;
    private String[] entities;

    public DatamartDependency() {
    }

    public DatamartDependency(Integer statId, String[] entities) {
        this.statId = statId;
        this.entities = entities;
    }

    public Integer getStatId() {
        return statId;
    }

    public void setStatId(Integer statId) {
        this.statId = statId;
    }

    public String[] getEntities() {
        return entities;
    }

    public void setEntities(String[] entities) {
        this.entities = entities;
    }

    @Override
    public String toString() {
        return "DatamartDependency{" +
                "datamartsStatId=" + statId +
                ", datamarts=" + Arrays.toString(entities) +
                '}';
    }
}
