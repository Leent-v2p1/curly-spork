package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.config;

import lombok.SneakyThrows;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.CtlApiCalls;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.dto.LoadingWithStatusResponse;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.dto.Profile;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.SysPropertyTool;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.WfParam;

import java.util.Map;

public class CtlParameters {

    private final WfParam param;

    public CtlParameters(WfParam param) {
        this.param = param;
    }

    @SneakyThrows
    public Map<String, String> buildCtlParam() {

        String ctlUrl = SysPropertyTool.safeSystemProperty("spark.ctl.url");
        String loadingId = SysPropertyTool.safeSystemProperty("spark.ctl.loading.id");
        String wfId = SysPropertyTool.safeSystemProperty("spark.ctl.wf.id");
        String loadingStart = SysPropertyTool.safeSystemProperty("spark.ctl.loading.start");
        String customBuildDate = SysPropertyTool.getSystemProperty("spark.custom.build.date");

        CtlApiCalls apiCalls = param.getApiCalls();
        LoadingWithStatusResponse loading = apiCalls.getLoading(Integer.parseInt(loadingId));
        Profile profileString = apiCalls.getProfile(loading.profile);
        String hueUri = profileString.hueUri;
        String hueLink = loading.xid;
        String ctlApiVersionV5 = String.valueOf(param.getCtlApiVersionV5());

        Map<String, String> parametersFromCtl = param.get();
        parametersFromCtl.put("spark.ctl.api.version.v5", ctlApiVersionV5);
        parametersFromCtl.put("datamart.all.hueUri", hueUri);
        parametersFromCtl.put("datamart.all.hueLink", hueLink);
        parametersFromCtl.put("ctl.url", ctlUrl);
        parametersFromCtl.put("ctl.loading.id", loadingId);
        parametersFromCtl.put("ctl.wf.id", wfId);
        parametersFromCtl.put("ctl.loading.start", loadingStart);
        parametersFromCtl.put("custom.build.date", customBuildDate);
        return parametersFromCtl;
    }
}
