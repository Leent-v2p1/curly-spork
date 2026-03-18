package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.dto;

import java.util.Objects;

public class LoadingResponse {
    public final boolean auto;
    public final String start_dttm;
    public final String end_dttm;
    public final String xid;
    public final String alive;
    public final int id;
    public final String status_log;
    public final String status;
    public final int wf_id;

    public LoadingResponse(boolean auto, String start_dttm, String xid, String alive, int id, String status_log, String status, int wf_id) {
        this.auto = auto;
        this.start_dttm = start_dttm;
        this.xid = xid;
        this.alive = alive;
        this.id = id;
        this.status_log = status_log;
        this.status = status;
        this.wf_id = wf_id;
        this.end_dttm = "";
    }

    public LoadingResponse(boolean auto,
                           String start_dttm,
                           String end_dttm,
                           String xid,
                           String alive,
                           int id,
                           String status_log,
                           String status,
                           int wf_id) {
        this.auto = auto;
        this.start_dttm = start_dttm;
        this.end_dttm = end_dttm;
        this.xid = xid;
        this.alive = alive;
        this.id = id;
        this.status_log = status_log;
        this.status = status;
        this.wf_id = wf_id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LoadingResponse that = (LoadingResponse) o;
        return auto == that.auto &&
                id == that.id &&
                wf_id == that.wf_id &&
                Objects.equals(start_dttm, that.start_dttm) &&
                Objects.equals(end_dttm, that.end_dttm) &&
                Objects.equals(xid, that.xid) &&
                Objects.equals(alive, that.alive) &&
                Objects.equals(status_log, that.status_log) &&
                Objects.equals(status, that.status);
    }

    @Override
    public int hashCode() {
        return Objects.hash(auto, start_dttm, end_dttm, xid, alive, id, status_log, status, wf_id);
    }

    @Override
    public String toString() {
        return "LoadingResponse{" +
                "auto=" + auto +
                ", start_dttm='" + start_dttm + '\'' +
                ", end_dttm='" + end_dttm + '\'' +
                ", xid='" + xid + '\'' +
                ", alive='" + alive + '\'' +
                ", id=" + id +
                ", status_log='" + status_log + '\'' +
                ", status='" + status + '\'' +
                ", wf_id=" + wf_id +
                '}';
    }
}
