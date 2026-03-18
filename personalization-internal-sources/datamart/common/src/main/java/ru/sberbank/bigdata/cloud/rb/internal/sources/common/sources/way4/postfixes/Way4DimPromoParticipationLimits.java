package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.way4.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class Way4DimPromoParticipationLimits implements SourcePostfix {

    @Override
    public String getPath() {
        return "dim-promo-participation-limits";
    }

    @Override
    public String getPostfix() {
        return "dim-promo-participation-limits";
    }
}
