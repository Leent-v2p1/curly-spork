package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.way4.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class Way4GlTraceRb implements SourcePostfix {

    @Override
    public String getPath() {
        return "gl-trace-rb";
    }

    @Override
    public String getPostfix() {
        return "gl-trace-rb";
    }
}
