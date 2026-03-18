package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class TariffUnion implements SourcePostfix {

    @Override
    public String getPath() {
        return "tariffUnion";
    }

    @Override
    public String getPostfix() {
        return "tariff-union";
    }
}