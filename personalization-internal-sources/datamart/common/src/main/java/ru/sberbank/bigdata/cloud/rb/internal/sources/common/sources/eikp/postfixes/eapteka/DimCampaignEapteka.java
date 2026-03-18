package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.eikp.postfixes.eapteka;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class DimCampaignEapteka implements SourcePostfix {

    @Override
    public String getPostfix() {
        return "dim-campaign-eapteka-daily";
    }

    @Override
    public String getPath() {
        return "dim-campaign-eapteka";
    }
}
