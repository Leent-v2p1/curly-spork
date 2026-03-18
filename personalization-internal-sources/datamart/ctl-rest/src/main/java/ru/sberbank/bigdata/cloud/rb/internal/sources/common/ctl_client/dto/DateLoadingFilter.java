package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.dto;

import java.util.List;

public class DateLoadingFilter {
    public final int wf_id;
    public final String start;
    public final String end;
    public final List<String> status;

    public DateLoadingFilter(int wf_id, String end, List<String> status) {
        this.wf_id = wf_id;
        this.start = null;
        this.end = end;
        this.status = status;
    }

    @Override
    public String toString() {
        return "DateLoadingFilter{" +
                "wf_id=" + wf_id +
                ", start='" + start + '\'' +
                ", end='" + end + '\'' +
                ", status=" + status +
                '}';
    }
}
