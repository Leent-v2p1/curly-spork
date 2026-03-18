package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.CodSourcePostfix;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.CodTerbankCode;

import java.util.Arrays;

public class DepAgrmntAggrPreInit extends CodSourcePostfix {

    private DepAgrmntAggrPreInit(CodTerbankCode code) {
        super(code, "dep-agrmnt-aggr-preparing-init", "depAgrmntAggrPreInit");
    }

    public static DepAgrmntAggrPreInit[] values() {
        return Arrays.stream(CodTerbankCode.values())
                .map(DepAgrmntAggrPreInit::new)
                .toArray(DepAgrmntAggrPreInit[]::new);
    }
}
