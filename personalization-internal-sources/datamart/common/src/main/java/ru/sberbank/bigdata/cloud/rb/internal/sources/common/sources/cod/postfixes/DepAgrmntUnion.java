package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class DepAgrmntUnion implements SourcePostfix {

    @Override
    public String getPath() {
        return "depAgrmntUnion";
    }

    @Override
    public String getPostfix() {
        return "dep-agrmnt-union";
    }
}
