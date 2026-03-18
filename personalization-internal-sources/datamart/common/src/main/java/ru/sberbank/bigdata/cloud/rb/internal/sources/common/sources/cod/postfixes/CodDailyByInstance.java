package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.CodSourcePostfix;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.CodTerbankCode;

import java.util.Arrays;

public class CodDailyByInstance extends CodSourcePostfix {

    public CodDailyByInstance(CodTerbankCode code) {
        super(code, "daily", "byInstanceDaily");
    }

    public static CodDailyByInstance[] values() {
        return Arrays.stream(CodTerbankCode.values())
                .map(CodDailyByInstance::new)
                .toArray(CodDailyByInstance[]::new);
    }
}
