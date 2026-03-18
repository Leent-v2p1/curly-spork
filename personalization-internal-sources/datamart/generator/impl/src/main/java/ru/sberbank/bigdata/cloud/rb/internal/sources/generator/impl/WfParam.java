package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl;

import org.json.JSONException;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.CtlApiCalls;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.CtlApiCallsV1;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.CtlApiCallsV5;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.dto.LoadingWithStatusResponse;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.dto.Workflow;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.dto.WorkflowParam;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.SysPropertyTool;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.PropertyConstants.DEFAULT_VALUE_CTL_VERSION_V5;


public class WfParam {

    public final CtlApiCalls apiCalls;
    public final Integer loadingId;
    private boolean ctlApiVersionV5 = false;

    public WfParam(String ctlUrl, Integer loadingId) {
        ctlApiVersionV5 = Boolean.parseBoolean(SysPropertyTool.getSystemProperty("spark.ctl.api.version.v5", DEFAULT_VALUE_CTL_VERSION_V5));
        if (ctlApiVersionV5){
            this.apiCalls = new CtlApiCallsV5(ctlUrl);
        } else {
            this.apiCalls = new CtlApiCallsV1(ctlUrl);
        }
        this.loadingId = loadingId;
    }

    public WfParam(CtlApiCalls apiCalls, Integer loadingId) {
        this.apiCalls = apiCalls;
        this.loadingId = loadingId;
    }
    public boolean getCtlApiVersionV5(){
        return this.ctlApiVersionV5;
    }

    public CtlApiCalls getApiCalls(){
        return this.apiCalls;
    }

    public static WfParam createFromSystemProperties() {
        String ctlUrl = SysPropertyTool.safeSystemProperty("spark.ctl.url");
        String loadingId = SysPropertyTool.safeSystemProperty("spark.ctl.loading.id");
        return new WfParam(ctlUrl, Integer.parseInt(loadingId));
    }

    public Map<String, String> get() throws JSONException, IOException {
        LoadingWithStatusResponse loading = apiCalls.getLoading(loadingId);

        Workflow workflow = loading.workflow;
        List<WorkflowParam> param = workflow.getParam();
        return param.stream()
                .collect(Collectors.toMap(WorkflowParam::getParam, WorkflowParam::getPrior_value));
    }
}
