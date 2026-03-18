package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.eikp.postfixes.sbmkt;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class DimTargetSbmkt implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "dim-target-sbmkt-daily";
    }
    @Override
    public String getPath() {
        return "dim-target-sbmkt";
    }
}