package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client;

public enum CtlLoadingStatus {
    ERROR("ERROR"),
    PARAM("PARAM"),
    TIME_WAIT("TIME-WAIT"),
    PREREQ("PREREQ"),
    LOCK_WAIT("LOCK-WAIT"),
    LOCK("LOCK"),
    RUNNING("RUNNING"),
    SUCCESS("SUCCESS"),
    EVENT_WAIT("EVENT-WAIT");

    private final String ctlRestApiValue;

    CtlLoadingStatus(String ctlRestApiValue) {
        this.ctlRestApiValue = ctlRestApiValue;
    }

    public String getCtlRestApiValue() {
        return ctlRestApiValue;
    }
}
