package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.dto;

import java.util.List;

public class LoadingFilter {

    public final int wf_id;
    public final List<String> alive;
    public final List<String> status;

    public LoadingFilter(int wf_id, List<String> alive, List<String> status) {
        this.wf_id = wf_id;
        this.alive = alive;
        this.status = status;
    }
}
