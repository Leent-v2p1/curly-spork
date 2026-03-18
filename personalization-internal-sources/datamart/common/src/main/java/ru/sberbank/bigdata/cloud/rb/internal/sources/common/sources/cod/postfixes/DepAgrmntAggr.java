package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.CodSourcePostfix;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.CodTerbankCode;

import java.util.Arrays;

public class DepAgrmntAggr extends CodSourcePostfix {

    private DepAgrmntAggr(CodTerbankCode code) {
        super(code, "dep-agrmnt-aggr", "depAgrmntAggr");
    }

    public static DepAgrmntAggr[] values() {
        return Arrays.stream(CodTerbankCode.values())
                .map(DepAgrmntAggr::new)
                .toArray(DepAgrmntAggr[]::new);
    }
}
