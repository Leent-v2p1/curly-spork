package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.way4.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class Way4ScdCardPprb implements SourcePostfix {

    @Override
    public String getPath() {
        return "scd-card-pprb";
    }

    @Override
    public String getPostfix() {
        return "scd-card-pprb";
    }
}
