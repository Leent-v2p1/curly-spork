package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.ekp.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class EkpCredDepositDaily implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "cred-deposit-daily";
    }

    @Override
    public String getPath() {
        return "cred-deposit-daily";
    }
}
