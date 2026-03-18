package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.erib.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.erib.EribInstance;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.erib.EribSourcePostfix;

import java.util.Arrays;

public class EribOper extends EribSourcePostfix {

    public EribOper(EribInstance instance) {
        super(instance, "oper", "byInstance-oper");
    }

    public static EribOper[] values() {
        return Arrays.stream(EribInstance.values())
                .map(EribOper::new)
                .toArray(EribOper[]::new);
    }
}
