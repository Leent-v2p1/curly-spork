package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.eikp.postfixes.sbermobail;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class DimCampaignSbermobail implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "dim-campaign-sbermobail-daily";
    }

    @Override
    public String getPath() {
        return "dim-campaign-sbermobail";
    }
}
