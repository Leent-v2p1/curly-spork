package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.CodSourcePostfix;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.CodTerbankCode;

import java.util.Arrays;

public class DepAgrmntSalary extends CodSourcePostfix {

    private DepAgrmntSalary(CodTerbankCode code) {
        super(code, "dep-agrmnt-salary", "depAgrmntSalary");
    }

    public static DepAgrmntSalary[] values() {
        return Arrays.stream(CodTerbankCode.values())
                .map(DepAgrmntSalary::new)
                .toArray(DepAgrmntSalary[]::new);
    }
}
