package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.prereqs.entites;

import java.util.Arrays;

public class CommonDependency {
    private Integer statId;
    private String profile;
    private Integer[] entities;

    public CommonDependency() {
    }

    public CommonDependency(Integer statId, String profile, Integer[] entities) {
        this.statId = statId;
        this.profile = profile;
        this.entities = entities;
    }

    public void setStatId(Integer statId) {
        this.statId = statId;
    }

    public void setProfile(String profile) {
        this.profile = profile;
    }

    public void setEntities(Integer[] entities) {
        this.entities = entities;
    }

    public Integer getStatId() {
        return statId;
    }

    public Integer[] getEntities() {
        return entities;
    }

    public String getProfile() {
        return profile;
    }

    @Override
    public String toString() {
        return "CommonDependency{" +
                "statId=" + statId +
                ", profile='" + profile + '\'' +
                ", entities=" + Arrays.toString(entities) +
                '}';
    }
}