package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class PprbTariffDaily implements SourcePostfix {

    @Override
    public String getPath() {
        return "tariffPprbDaily";
    }

    @Override
    public String getPostfix() {
        return "tariff-pprb-daily";
    }
}
