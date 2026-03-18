package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.way4.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class Way4Nsi implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "nsi";
    }

    @Override
    public String getPath() {
        return "nsi";
    }
}
