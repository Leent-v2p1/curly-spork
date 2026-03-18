package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class FtDepTxnAggrUnion implements SourcePostfix {

    @Override
    public String getPath() {return "ftDepTxnAggrUnion";}

    @Override
    public String getPostfix() {
        return "ft-dep-txn-aggr-union";
    }
}
