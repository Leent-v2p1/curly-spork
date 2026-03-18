package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.greenplum.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class GreenplumFtEcosysTxnDetMover implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "ft-ecosys-txn-det-mover-daily";
    }

    @Override
    public String getPath() {
        return "FtEcosysTxnDetMover";
    }
}