package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.prereqs.entites;

public class Dependencies {
    private CommonDependency[] replicas;
    private DatamartDependency[] datamarts;
    private String cron;

    public Dependencies() {
        //empty constructor because its java bean and used by org.yaml.snakeyaml.Yaml
    }

    public CommonDependency[] getReplicas() {
        return replicas;
    }

    public void setReplicas(CommonDependency[] replicas) {
        this.replicas = replicas;
    }

    public DatamartDependency[] getDatamarts() {
        return datamarts;
    }

    public void setDatamarts(DatamartDependency[] datamarts) {
        this.datamarts = datamarts;
    }

    public String getCron() {
        return cron;
    }

    public void setCron(String cron) {
        this.cron = cron;
    }
}
