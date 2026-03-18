package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.way4.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class Way4CredLimitHistDaily implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "cred-limit-hist-daily";
    }

    @Override
    public String getPath() {
        return "cred-limit-hist-daily";
    }
}
