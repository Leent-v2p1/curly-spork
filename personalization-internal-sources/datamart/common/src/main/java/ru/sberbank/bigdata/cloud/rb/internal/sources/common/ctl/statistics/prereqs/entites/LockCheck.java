package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.prereqs.entites;

import java.util.Objects;

public class LockCheck {
    private String entityId;
    private String type;
    private String profile;
    private String lockGroup = "INIT";

    public LockCheck() {
    }

    public LockCheck(String entityId, String type, String profile, String lockGroup) {
        this.entityId = entityId;
        this.type = type;
        this.profile = profile;
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
        LockCheck lock = (LockCheck) o;
        return Objects.equals(entityId, lock.entityId) &&
                Objects.equals(type, lock.type) &&
                Objects.equals(profile, lock.profile) &&
                Objects.equals(lockGroup, lock.lockGroup);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entityId, type, profile, lockGroup);
    }

    @Override
    public String toString() {
        return "LockCheck{" +
                "entityId='" + entityId + '\'' +
                ", type='" + type + '\'' +
                ", profile='" + profile + '\'' +
                ", lockGroup='" + lockGroup + '\'' +
                '}';
    }
}
