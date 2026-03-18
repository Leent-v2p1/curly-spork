package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.greenplum.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class GreenplumEvkCampaignDicDaily implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "evk-campaign-dic-daily";
    }

    @Override
    public String getPath() {
        return "EvkCampaignDic";
    }
}
