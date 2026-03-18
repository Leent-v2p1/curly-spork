package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.way4.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class Way4DMAcctStateMonthly implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "dm-acct-state-monthly";
    }

    @Override
    public String getPath() {
        return "dm-acct-state-monthly";
    }
}
