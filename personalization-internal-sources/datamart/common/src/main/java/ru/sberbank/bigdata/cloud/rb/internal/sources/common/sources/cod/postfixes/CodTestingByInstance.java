package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.CodSourcePostfix;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.CodTerbankCode;

import java.util.Arrays;

public class CodTestingByInstance extends CodSourcePostfix {

    public CodTestingByInstance(CodTerbankCode code) {
        super(code, "testing", "testingByInstance");
    }

    public static CodTestingByInstance[] values() {
        return Arrays.stream(CodTerbankCode.values())
                .map(CodTestingByInstance::new)
                .toArray(CodTestingByInstance[]::new);
    }
}
