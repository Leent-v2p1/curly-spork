package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.rest;

public enum CtlLoadingStatus {
    INIT("INIT"),
    START("START"),
    TIMEWAIT("TIME-WAIT"),
    EVENTWAIT("EVENT-WAIT"),
    LOCKWAIT("LOCK-WAIT"),
    PREREQ("PREREQ"),
    LOCK("LOCK"),
    PARAM("PARAM"),
    RUNNING("RUNNING"),
    SUCCESS("SUCCESS"),
    ERROR("ERROR"),
    ERRORCHECK("ERRORCHECK");

    private String status;

    private CtlLoadingStatus(String status) {
        this.status = status;
    }

    public String toString() {
        return this.status;
    }
    public String getCtlRestApiValue() {
        return status;
    }
}
