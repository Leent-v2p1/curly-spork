package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.way4.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class Way4RefCardFirstTransact implements SourcePostfix {

    @Override
    public String getPath() {
        return "ref-card-first-transact";
    }

    @Override
    public String getPostfix() {
        return "ref-card-first-transact";
    }
}
