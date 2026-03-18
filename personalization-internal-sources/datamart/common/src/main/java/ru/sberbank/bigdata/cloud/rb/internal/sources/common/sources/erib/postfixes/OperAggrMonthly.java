package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.erib.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class OperAggrMonthly implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "oper-aggr-monthly";
    }

    @Override
    public String getPath() {
        return "operAggrMonthly";
    }
}
