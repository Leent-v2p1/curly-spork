package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.erib.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.erib.EribInstance;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.erib.EribSourcePostfix;

import java.util.Arrays;

public class EribLogonDaily extends EribSourcePostfix {

    public EribLogonDaily(EribInstance instance) {
        super(instance, "logon-daily", "byInstance-logon-daily");
    }

    public static EribLogonDaily[] values() {
        return Arrays.stream(EribInstance.values())
                .map(EribLogonDaily::new)
                .toArray(EribLogonDaily[]::new);
    }
}
