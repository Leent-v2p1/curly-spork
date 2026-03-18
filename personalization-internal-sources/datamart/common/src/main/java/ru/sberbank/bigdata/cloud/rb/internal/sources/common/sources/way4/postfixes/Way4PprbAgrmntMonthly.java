package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.way4.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class Way4PprbAgrmntMonthly implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "txn-union-monthly";
    }

    @Override
    public String getPath() {
        return "txn-union-monthly";
    }
}