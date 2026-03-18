package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.dto;

public class WorkflowParam {

    private final Integer wf_id;

    private final String param;

    private final String prior_value;

    public WorkflowParam(Integer wf_id, String param, String prior_value) {
        this.wf_id = wf_id;
        this.param = param;
        this.prior_value = prior_value;
    }

    public Integer getWf_id() {
        return wf_id;
    }

    public String getParam() {
        return param;
    }

    public String getPrior_value() {
        return prior_value;
    }
}
