package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.way4.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class Way4MerchWay4Full implements SourcePostfix {

    @Override
    public String getPath() {
        return "merchants-way-full";
    }

    @Override
    public String getPostfix() {
        return "merchants-way-full";
    }
}
