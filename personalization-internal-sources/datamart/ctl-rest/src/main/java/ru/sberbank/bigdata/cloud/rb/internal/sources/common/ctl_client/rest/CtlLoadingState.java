package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.rest;

public enum CtlLoadingState {
    ALIVE("ALIVE"),
    COMPLETED("COMPLETED"),
    ABORTED("ABORTED");

    private String state;

    private CtlLoadingState(String state) {
        this.state = state;
    }

    public String toString() {
        return this.state;
    }
}
