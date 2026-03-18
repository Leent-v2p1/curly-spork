package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.erib.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.erib.EribInstance;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.erib.EribSourcePostfix;

import java.util.Arrays;

public class EribOperUpd extends EribSourcePostfix {

    public EribOperUpd(EribInstance instance) {
        super(instance, "oper-upd", "byInstance-oper-upd");
    }

    public static EribOperUpd[] values() {
        return Arrays.stream(EribInstance.values())
                .map(EribOperUpd::new)
                .toArray(EribOperUpd[]::new);
    }
}
