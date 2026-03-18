package ru.sberbank.bigdata.cloud.rb.internal.sources.reporter;

public enum Ctl {
    PROD("http://septu4.df.sbrf.ru:8079"),
    DEV("http://hw2288-05.od-dev.omega.sbrf.ru:8080"),
    BDA("http://fada40.cloud.df.sbrf.ru:8888");

    private String ctlUrl;

    Ctl(String ctlUrl) {
        this.ctlUrl = ctlUrl;
    }

    public String getCtlUrl() {
        return ctlUrl;
    }
}
