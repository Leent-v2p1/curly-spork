package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.erib.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.erib.EribInstance;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.erib.EribSourcePostfix;

import java.util.Arrays;

public class EribOperAhd extends EribSourcePostfix {

    public EribOperAhd(EribInstance instance) {
        super(instance, "oper-ahd", "byInstance-oper-ahd");
    }

    public static EribOperAhd[] values() {
        return Arrays.stream(EribInstance.values())
                .map(EribOperAhd::new)
                .toArray(EribOperAhd[]::new);
    }
}
