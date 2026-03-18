package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.prereqs.entites;

public class Cron {
    private String expression;
    private boolean active;

    public Cron() {
    }

    public Cron(String expression, boolean active) {
        this.expression = expression;
        this.active = active;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public String getExpression() {
        return expression;
    }

    public boolean isActive() {
        return active;
    }

    @Override
    public String toString() {
        return "Cron{" +
                "expression='" + expression + '\'' +
                ", active=" + active +
                '}';
    }
}
