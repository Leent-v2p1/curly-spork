package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.CodSourcePostfix;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.CodTerbankCode;

import java.util.Arrays;

public class DepTxnAggr extends CodSourcePostfix {

    private DepTxnAggr(CodTerbankCode code) {
        super(code, "dep-txn-aggr-monthly", "depTxnAggr");
    }

    public static DepTxnAggr[] values() {
        return Arrays.stream(CodTerbankCode.values())
                .map(DepTxnAggr::new)
                .toArray(DepTxnAggr[]::new);
    }
}
