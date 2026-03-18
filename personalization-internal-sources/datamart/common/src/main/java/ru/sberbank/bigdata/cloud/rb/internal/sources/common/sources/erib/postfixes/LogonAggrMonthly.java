package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.erib.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class LogonAggrMonthly implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "logon-aggr-monthly";
    }

    @Override
    public String getPath() {
        return "logonAggrMonthly";
    }
}
