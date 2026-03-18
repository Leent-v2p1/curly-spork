package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.erib.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.erib.EribInstance;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.erib.EribSourcePostfix;

import java.util.Arrays;

public class EribDailyByInstance extends EribSourcePostfix {

    public EribDailyByInstance(EribInstance instance) {
        super(instance, "daily", "byInstance-daily");
    }

    public static EribDailyByInstance[] values() {
        return Arrays.stream(EribInstance.values())
                .map(EribDailyByInstance::new)
                .toArray(EribDailyByInstance[]::new);
    }
}
