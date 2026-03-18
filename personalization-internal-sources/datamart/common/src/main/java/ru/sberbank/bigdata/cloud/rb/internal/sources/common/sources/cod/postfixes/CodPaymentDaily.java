package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.CodSourcePostfix;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.CodTerbankCode;

import java.util.Arrays;

public class CodPaymentDaily extends CodSourcePostfix {

    private CodPaymentDaily(CodTerbankCode code) {
        super(code, "payment-daily", "paymentDaily");
    }

    public static CodPaymentDaily[] values() {
        return Arrays.stream(CodTerbankCode.values())
                .map(CodPaymentDaily::new)
                .toArray(CodPaymentDaily[]::new);
    }
}
