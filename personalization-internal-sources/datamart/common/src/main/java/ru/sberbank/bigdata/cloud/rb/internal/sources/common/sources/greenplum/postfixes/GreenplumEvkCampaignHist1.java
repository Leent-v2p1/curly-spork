package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.greenplum.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class GreenplumEvkCampaignHist1 implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "evk-campaign-hist1-daily";
    }

    @Override
    public String getPath() {
        return "EvkCampaignHist1";
    }
}