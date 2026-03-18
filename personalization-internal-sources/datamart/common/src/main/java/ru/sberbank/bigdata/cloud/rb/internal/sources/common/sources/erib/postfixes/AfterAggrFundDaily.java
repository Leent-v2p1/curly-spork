package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.erib.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.erib.EribInstance;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.erib.EribSourcePostfix;

import java.util.Arrays;

public class AfterAggrFundDaily extends EribSourcePostfix {

    public AfterAggrFundDaily(EribInstance instance) {
        super(instance, "after-aggr-fund-daily", "byInstance-AfterAggrFundDaily");
    }

    public static AfterAggrFundDaily[] values() {
        return Arrays.stream(EribInstance.values())
                .map(AfterAggrFundDaily::new)
                .toArray(AfterAggrFundDaily[]::new);
    }
}
