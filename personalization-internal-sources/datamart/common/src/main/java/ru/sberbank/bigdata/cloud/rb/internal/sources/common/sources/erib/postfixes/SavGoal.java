package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.erib.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.erib.EribInstance;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.erib.EribSourcePostfix;

import java.util.Arrays;

public class SavGoal extends EribSourcePostfix {

    public SavGoal(EribInstance instance) {
        super(instance, "sav-goal", "byInstance-sav-goal");
    }

    public static SavGoal[] values() {
        return Arrays.stream(EribInstance.values())
                .map(SavGoal::new)
                .toArray(SavGoal[]::new);
    }
}