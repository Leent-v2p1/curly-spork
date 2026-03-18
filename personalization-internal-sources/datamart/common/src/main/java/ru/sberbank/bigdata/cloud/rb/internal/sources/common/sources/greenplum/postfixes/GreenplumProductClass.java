package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.greenplum.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class GreenplumProductClass implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "product-class-daily";
    }

    @Override
    public String getPath() {
        return "VnvProductClass";
    }
}
