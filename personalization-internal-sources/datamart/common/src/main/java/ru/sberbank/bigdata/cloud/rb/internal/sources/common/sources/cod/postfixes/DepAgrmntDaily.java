package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.CodSourcePostfix;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.CodTerbankCode;

import java.util.Arrays;

public class DepAgrmntDaily extends CodSourcePostfix {

    private DepAgrmntDaily(CodTerbankCode code) {
        super(code, "dep-agrmnt-daily", "depAgrmntDaily");
    }

    public static DepAgrmntDaily[] values() {
        return Arrays.stream(CodTerbankCode.values())
                .map(DepAgrmntDaily::new)
                .toArray(DepAgrmntDaily[]::new);
    }
}
