package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.dto;

import java.util.List;

public class LoadingWithStatusResponse extends LoadingResponse {
    public final Workflow workflow;
    public final List<LoadingStatus> loading_status;
    public final String profile;

    public LoadingWithStatusResponse(boolean auto,
                                     Workflow workflow,
                                     String start_dttm,
                                     String end_dttm,
                                     String xid,
                                     String alive,
                                     int id,
                                     String status_log,
                                     String status,
                                     int wf_id,
                                     String profile,
                                     List<LoadingStatus> loading_status) {
        super(auto, start_dttm, end_dttm, xid, alive, id, status_log, status, wf_id);
        this.workflow = workflow;
        this.loading_status = loading_status;
        this.profile = profile;
    }
}
