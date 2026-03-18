package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.way4.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class Way4DimPromoTariffOverrides implements SourcePostfix {

    @Override
    public String getPath() {
        return "dim-promo-tariff-overrides";
    }

    @Override
    public String getPostfix() {
        return "dim-promo-tariff-overrides";
    }
}
