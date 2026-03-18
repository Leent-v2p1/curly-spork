package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.ekp.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class EkpAgrmntBalDaily implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "agrmnt-bal-daily";
    }

    @Override
    public String getPath() {
        return "agrmnt-bal-daily";
    }
}
