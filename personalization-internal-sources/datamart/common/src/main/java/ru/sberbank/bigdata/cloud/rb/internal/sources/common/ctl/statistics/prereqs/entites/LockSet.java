package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.prereqs.entites;

import java.util.Objects;

public class LockSet {
    private String entityId;
    private String type;
    private String profile;
    private String estimate;
    private String lockGroup = "INIT";

    public LockSet() {
    }

    public LockSet(String entityId, String type, String profile, String estimate, String lockGroup) {
        this.entityId = entityId;
        this.type = type;
        this.profile = profile;
        this.estimate = estimate;
        this.lockGroup = lockGroup;
    }

    public String getEntityId() {
        return entityId;
    }

    public void setEntityId(String entityId) {
        this.entityId = entityId;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getProfile() {
        return profile;
    }

    public void setProfile(String profile) {
        this.profile = profile;
    }

    public String getEstimate() {
        return estimate;
    }

    public void setEstimate(String estimate) {
        this.estimate = estimate;
    }

    public String getLockGroup() {
        return lockGroup;
    }

    public void setLockGroup(String lockGroup) {
        this.lockGroup = lockGroup;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LockSet lockSet = (LockSet) o;
        return Objects.equals(entityId, lockSet.entityId) &&
                Objects.equals(type, lockSet.type) &&
                Objects.equals(profile, lockSet.profile) &&
                Objects.equals(estimate, lockSet.estimate) &&
                Objects.equals(lockGroup, lockSet.lockGroup);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entityId, type, profile, estimate, lockGroup);
    }

    @Override
    public String toString() {
        return "LockSet{" +
                "entityId='" + entityId + '\'' +
                ", type='" + type + '\'' +
                ", profile='" + profile + '\'' +
                ", estimate='" + estimate + '\'' +
                ", lockGroup='" + lockGroup + '\'' +
                '}';
    }
}
