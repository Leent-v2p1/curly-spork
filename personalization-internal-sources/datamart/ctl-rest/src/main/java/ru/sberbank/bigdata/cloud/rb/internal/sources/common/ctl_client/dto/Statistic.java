package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.dto;

public class Statistic {
    public final int loading_id;
    public final String profile;
    public final int entity_id;
    public final int stat_id;
    public final String value;

    public Statistic(int loading_id, int entity_id, int stat_id, String value) {
        this(loading_id, "default", entity_id, stat_id, value);
    }

    public Statistic(int loading_id, String profile, int entity_id, int stat_id, String value) {
        this.loading_id = loading_id;
        this.profile = profile;
        this.entity_id = entity_id;
        this.stat_id = stat_id;
        this.value = value;
    }

    public int getLoading_id() {
        return loading_id;
    }

    public String getProfile() {
        return profile;
    }

    public int getStat_id() {
        return stat_id;
    }

    public int getEntity_id() {
        return entity_id;
    }

    public String getValue() {
        return value;
    }
}
