package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics;

import java.util.Arrays;

public enum StatisticId {

    LAST_LOAD_START(1),
    CHANGE_STAT_ID(2),
    BUSINESS_DATE_STAT_ID(5),
    MONTH_WF_SCHEDULE(6),
    ROLLBACK(9),
    CSN(10),
    LAST_LOADED_STAT_ID(11),
    PROCESSED_LOADING_ID(19),
    LOADING_TYPE(34);

    public static final String CHANGE_STAT_VALUE = "true";
    private final int code;

    StatisticId(int code) {
        this.code = code;
    }

    public static StatisticId fromCode(int statisticCode) {
        return Arrays.stream(StatisticId.values())
                .filter(statisticId -> statisticId.code == statisticCode)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No such code: " + statisticCode));
    }

    public int getCode() {
        return code;
    }
}
