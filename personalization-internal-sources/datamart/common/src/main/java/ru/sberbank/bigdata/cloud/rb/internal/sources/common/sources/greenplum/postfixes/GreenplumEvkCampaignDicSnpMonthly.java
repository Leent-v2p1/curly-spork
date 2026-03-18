package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.greenplum.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class GreenplumEvkCampaignDicSnpMonthly implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "evk-campaign-dic-snp-monthly";
    }

    @Override
    public String getPath() {
        return "ref-union-campaign-dic-hdp-snp";
    }
}
