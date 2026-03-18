package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.dto;

public class LoadingStatus {
    public final int loading_id;
    public final String effective_from;
    public final String status;
    public final String log;

    public LoadingStatus(int loading_id, String effective_from, String status, String log) {
        this.loading_id = loading_id;
        this.effective_from = effective_from;
        this.status = status;
        this.log = log;
    }

    @Override
    public String toString() {
        return "LoadingStatus{" +
                "loading_id=" + loading_id +
                ", effective_from='" + effective_from + '\'' +
                ", status='" + status + '\'' +
                ", log='" + log + '\'' +
                '}';
    }
}
