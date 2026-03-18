package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.CodSourcePostfix;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.CodTerbankCode;

import java.util.Arrays;

public class IdStaging extends CodSourcePostfix {

    private IdStaging(CodTerbankCode code) {
        super(code, "stage-daily", "stageDaily");
    }

    public static IdStaging[] values() {
        return Arrays.stream(CodTerbankCode.values())
                .map(IdStaging::new)
                .toArray(IdStaging[]::new);
    }
}
