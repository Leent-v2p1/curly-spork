package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class CodTesting implements SourcePostfix {

    @Override
    public String getPostfix() {
        return "testing";
    }

    @Override
    public String getPath() {
        return "testing";
    }
}
