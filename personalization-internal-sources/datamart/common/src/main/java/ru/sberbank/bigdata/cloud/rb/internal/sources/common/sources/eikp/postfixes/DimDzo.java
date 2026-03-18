package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.eikp.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class DimDzo implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "dim-dzo";
    }

    @Override
    public String getPath() {
        return "dim-dzo";
    }
}
