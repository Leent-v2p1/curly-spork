package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.eikp.postfixes.sberzvuk;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class DimCampaignSberzvuk implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "dim-campaign-sberzvuk-daily";
    }
    @Override
    public String getPath() {
        return "dim-campaign-sberzvuk";
    }
}