package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.way4.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class Way4UnionTxnPprb implements SourcePostfix {

    @Override
    public String getPath() {
        return "ft-txn-det-union";
    }

    @Override
    public String getPostfix() {
        return "ft-txn-det-union";
    }
}
