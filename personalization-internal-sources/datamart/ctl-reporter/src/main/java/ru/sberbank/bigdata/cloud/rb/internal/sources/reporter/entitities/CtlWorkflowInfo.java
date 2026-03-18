package ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.entitities;

import java.util.Objects;

public class CtlWorkflowInfo {
    public final String id;
    public final String system;
    public final String workflowName;
    public final String isUsed;

    public CtlWorkflowInfo(String id, String system, String workflowName, String isUsed) {
        this.id = id;
        this.system = system;
        this.workflowName = workflowName;
        this.isUsed = isUsed;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CtlWorkflowInfo that = (CtlWorkflowInfo) o;
        return Objects.equals(id, that.id) &&
                Objects.equals(system, that.system) &&
                Objects.equals(workflowName, that.workflowName) &&
                Objects.equals(isUsed, that.isUsed);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, system, workflowName, isUsed);
    }
}
