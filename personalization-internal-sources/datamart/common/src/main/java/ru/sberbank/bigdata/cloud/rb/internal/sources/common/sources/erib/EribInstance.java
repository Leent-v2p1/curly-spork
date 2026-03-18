package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.erib;

import static java.util.Arrays.asList;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.functions.CollectionUtils.extractExactlyOneElement;

public enum EribInstance {
    IKFL(1),
    IKFL2(2),
    IKFL3(3),
    IKFL4(4),
    IKFL5(5),
    IKFL6(6),
    IKFL7(7),
    IKFL_GF(0);

    private final int code;

    EribInstance(int code) {
        this.code = code;
    }

    public static EribInstance find(String nameWithPostfix) {
        return extractExactlyOneElement(
                asList(values()),
                eribSourceSchemaPostfix -> nameWithPostfix.endsWith(eribSourceSchemaPostfix.name().toLowerCase()));
    }

    public int code() {
        return code;
    }
}
