package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.ekp.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class EkpOperDaily implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "oper-daily";
    }

    @Override
    public String getPath() {
        return "oper-daily";
    }
}
