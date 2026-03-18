package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class DepTxnUnion implements SourcePostfix {

    @Override
    public String getPath() {
        return "depTxnUnion";
    }

    @Override
    public String getPostfix() {
        return "dep-txn-union";
    }
}
