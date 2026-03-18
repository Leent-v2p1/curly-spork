package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.prereqs.entites;

import java.util.Arrays;

public class Locks {
    private LockCheck[] checks;
    private LockSet[] sets;

    public LockCheck[] getChecks() {
        return checks;
    }

    public void setChecks(LockCheck[] checks) {
        this.checks = checks;
    }

    public LockSet[] getSets() {
        return sets;
    }

    public void setSets(LockSet[] sets) {
        this.sets = sets;
    }

    @Override
    public String toString() {
        return "Locks{" +
                "checks=" + Arrays.toString(checks) +
                ", sets=" + Arrays.toString(sets) +
                '}';
    }
}
