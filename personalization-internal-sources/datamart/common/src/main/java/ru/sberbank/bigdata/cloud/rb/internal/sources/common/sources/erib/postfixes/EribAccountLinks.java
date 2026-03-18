package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.erib.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class EribAccountLinks implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "account-links";
    }

    @Override
    public String getPath() {
        return "eribAccountLinks";
    }
}
