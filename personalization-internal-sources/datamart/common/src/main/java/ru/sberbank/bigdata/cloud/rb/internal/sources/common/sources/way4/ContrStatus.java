package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.way4;

import java.util.Arrays;

public enum ContrStatus {
    CARD_OPEN(14),
    IS_NOT_ISSUED(223),
    RENEWED_NOT_ISSUED(239);

    public final int code;

    ContrStatus(int code) {
        this.code = code;
    }

    public static Integer[] codes() {
        return Arrays.stream(values())
                .map(contrStatus -> contrStatus.code)
                .toArray(Integer[]::new);
    }
}
