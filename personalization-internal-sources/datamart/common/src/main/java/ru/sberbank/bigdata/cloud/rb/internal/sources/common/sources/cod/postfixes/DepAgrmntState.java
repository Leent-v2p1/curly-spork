package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.CodSourcePostfix;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.CodTerbankCode;

import java.util.Arrays;

public class DepAgrmntState extends CodSourcePostfix {

    private DepAgrmntState(CodTerbankCode code) {
        super(code, "state-daily", "depAgrmntState");
    }

    public static DepAgrmntState[] values() {
        return Arrays.stream(CodTerbankCode.values())
                .map(DepAgrmntState::new)
                .toArray(DepAgrmntState[]::new);
    }
}
