package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.dto;

import java.util.Objects;

public class CtlInfo {

    private final String version;
    private final String commit;
    private final String branch;

    public CtlInfo(String version, String commit, String branch) {
        this.version = version;
        this.commit = commit;
        this.branch = branch;
    }

    public String getVersion() {
        return version;
    }

    public String getCommit() {
        return commit;
    }

    public String getBranch() {
        return branch;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CtlInfo)) {
            return false;
        }
        CtlInfo ctlInfo = (CtlInfo) o;
        return Objects.equals(version, ctlInfo.version) &&
                Objects.equals(commit, ctlInfo.commit) &&
                Objects.equals(branch, ctlInfo.branch);
    }

    @Override
    public int hashCode() {
        return Objects.hash(version, commit, branch);
    }

    @Override
    public String toString() {
        return "CtlInfo{" +
                "version='" + version + '\'' +
                ", commit='" + commit + '\'' +
                ", branch='" + branch + '\'' +
                '}';
    }
}

