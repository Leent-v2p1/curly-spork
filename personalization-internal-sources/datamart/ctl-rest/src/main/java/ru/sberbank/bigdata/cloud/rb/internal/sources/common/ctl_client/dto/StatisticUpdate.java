package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.dto;

import java.util.List;

public class StatisticUpdate {
    public final int loading_id;
    public final int entity_id;
    public final int stat_id;
    public final List<String> avalue;

    public StatisticUpdate(int loading_id, int entity_id, int stat_id, List<String> avalue) {
        this.loading_id = loading_id;
        this.entity_id = entity_id;
        this.stat_id = stat_id;
        this.avalue = avalue;
    }
}
