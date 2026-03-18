package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.ekp.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class EkpCurrencyRateDaily implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "currency-rate-daily";
    }

    @Override
    public String getPath() {
        return "currency-rate-daily";
    }
}
