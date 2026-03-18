package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.greenplum.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class GreenplumEvkCampaignHist2Mover implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "evk-campaign-hist2-mover-daily";
    }

    @Override
    public String getPath() {
        return "EvkCampaignHist2Mover";
    }
}