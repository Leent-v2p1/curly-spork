package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.eikp.postfixes.sbermobail;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class DimActionSbermobail implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "dim-action-sbermobail-daily";
    }

    @Override
    public String getPath() {
        return "dim-action-sbermobail";
    }
}
