package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.erib.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class OperHistDaily implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "oper-hist-daily";
    }

    @Override
    public String getPath() {
        return "operHist";
    }
}
