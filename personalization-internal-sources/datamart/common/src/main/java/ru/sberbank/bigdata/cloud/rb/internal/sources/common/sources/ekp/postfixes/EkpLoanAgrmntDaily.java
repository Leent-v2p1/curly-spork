package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.ekp.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class EkpLoanAgrmntDaily implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "loan-agrmnt-daily";
    }

    @Override
    public String getPath() {
        return "loan-agrmnt-daily";
    }
}