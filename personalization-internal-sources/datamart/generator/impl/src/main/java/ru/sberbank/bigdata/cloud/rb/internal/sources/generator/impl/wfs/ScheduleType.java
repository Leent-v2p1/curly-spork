package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.wfs;

public enum ScheduleType {
    SCHEDULE("on-schedule"),
    START("start"),
    NONE("none");

    public final String ctlClientString;

    ScheduleType(String ctlClientString) {
        this.ctlClientString = ctlClientString;
    }
}
