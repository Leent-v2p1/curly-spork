package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.CodSourcePostfix;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.CodTerbankCode;

import java.util.Arrays;

public class CodTariffDaily extends CodSourcePostfix {

    private CodTariffDaily(CodTerbankCode code) {
        super(code, "tariff-daily", "tariffDaily");
    }

    public static CodTariffDaily[] values() {
        return Arrays.stream(CodTerbankCode.values())
                .map(CodTariffDaily::new)
                .toArray(CodTariffDaily[]::new);
    }
}
