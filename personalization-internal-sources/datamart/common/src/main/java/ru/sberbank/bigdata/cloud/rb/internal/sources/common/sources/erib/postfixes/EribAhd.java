package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.erib.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.erib.EribInstance;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.erib.EribSourcePostfix;

import java.util.Arrays;

/**
 * required for recount_sbol, sbol_logon, sbol_oper datamarts
 */
public class EribAhd extends EribSourcePostfix {

    public EribAhd(EribInstance instance) {
        super(instance, "ahd", "byInstance-ahd");
    }

    public static EribAhd[] values() {
        return Arrays.stream(EribInstance.values())
                .map(EribAhd::new)
                .toArray(EribAhd[]::new);
    }
}
