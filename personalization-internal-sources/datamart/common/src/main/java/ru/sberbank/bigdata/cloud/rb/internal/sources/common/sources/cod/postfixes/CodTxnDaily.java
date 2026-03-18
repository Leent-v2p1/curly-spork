package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.CodSourcePostfix;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.CodTerbankCode;

import java.util.Arrays;

public class CodTxnDaily extends CodSourcePostfix {

    private CodTxnDaily(CodTerbankCode code) {
        super(code, "txn-daily", "txnDaily");
    }

    public static CodTxnDaily[] values() {
        return Arrays.stream(CodTerbankCode.values())
                .map(CodTxnDaily::new)
                .toArray(CodTxnDaily[]::new);
    }
}
