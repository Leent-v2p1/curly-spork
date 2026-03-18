package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.way4.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class Way4DimMerchants implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "dim-merchants";
    }

    @Override
    public String getPath() {
        return "dim-merchants";
    }
}
