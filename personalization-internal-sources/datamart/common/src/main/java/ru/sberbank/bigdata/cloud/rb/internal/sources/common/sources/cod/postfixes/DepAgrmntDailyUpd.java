package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.CodSourcePostfix;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.CodTerbankCode;

import java.util.Arrays;

public class DepAgrmntDailyUpd extends CodSourcePostfix {

    private DepAgrmntDailyUpd(CodTerbankCode code) {
        super(code, "dep-agrmnt-upd-daily", "depAgrmntDailyUpd");
    }

    public static DepAgrmntDailyUpd[] values() {
        return Arrays.stream(CodTerbankCode.values())
                .map(DepAgrmntDailyUpd::new)
                .toArray(DepAgrmntDailyUpd[]::new);
    }
}
