package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.prereqs.entites;

public class WorkflowParameters {

    private boolean skipIfBuiltToday = false;
    private String startDt;
    private String userName;

    public String getStartDt() {
        return startDt;
    }

    public void setStartDt(String startDt) {
        this.startDt = startDt;
    }

    public boolean getSkipIfBuiltToday() {
        return skipIfBuiltToday;
    }

    public void setSkipIfBuiltToday(boolean skipIfBuiltToday) {
        this.skipIfBuiltToday = skipIfBuiltToday;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    @Override
    public String toString() {
        return "WorkflowParameters{" +
                "skipIfBuiltToday=" + skipIfBuiltToday +
                ", startDt=" + startDt +
                ", userName=" + userName +
                '}';
    }
}
