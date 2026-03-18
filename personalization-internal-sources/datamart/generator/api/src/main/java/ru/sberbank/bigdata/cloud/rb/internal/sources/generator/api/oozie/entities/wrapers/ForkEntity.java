package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.wrapers;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.base.WorkflowType;

import static java.util.Collections.emptyList;

/**
 * Created by sbt-gladyshev-ana on 21.09.2017.
 */
public class ForkEntity extends ActionEntity {

    private final String forkName;
    private final String nextName;
    private final String parentName;

    public ForkEntity(String actionId, boolean first, String forkName, String nextName, String parentName) {
        super(actionId, first, WorkflowType.FORK, ActionType.NOT_ACTION, emptyList());
        this.forkName = forkName;
        this.nextName = nextName;
        this.parentName = parentName;
    }

    public String getNextName() {
        return nextName;
    }

    public String getForkName() {
        return forkName;
    }

    public String getParentName() {
        return parentName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        ForkEntity that = (ForkEntity) o;

        if (forkName != null ? !forkName.equals(that.forkName) : that.forkName != null) {
            return false;
        }
        if (nextName != null ? !nextName.equals(that.nextName) : that.nextName != null) {
            return false;
        }
        return parentName != null ? parentName.equals(that.parentName) : that.parentName == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (forkName != null ? forkName.hashCode() : 0);
        result = 31 * result + (nextName != null ? nextName.hashCode() : 0);
        result = 31 * result + (parentName != null ? parentName.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ForkEntity{" +
                "forkName='" + forkName + '\'' +
                ", nextName='" + nextName + '\'' +
                ", parentName='" + parentName + '\'' +
                '}';
    }
}
