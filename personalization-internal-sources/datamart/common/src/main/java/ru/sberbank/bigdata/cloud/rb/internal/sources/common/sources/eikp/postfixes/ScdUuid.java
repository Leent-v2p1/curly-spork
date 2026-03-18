package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.eikp.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class ScdUuid implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "scd-uuid";
    }

    @Override
    public String getPath() {
        return "scd-uuid";
    }
}
