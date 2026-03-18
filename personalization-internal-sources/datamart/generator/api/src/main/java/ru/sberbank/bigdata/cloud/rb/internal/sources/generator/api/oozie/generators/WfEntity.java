package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators;

public class WfEntity {
    private String name;
    private String appPath;
    private String nextName;
    private boolean firstBuild;
    private String customBuildDate;

    public WfEntity(String name, String appPath, String nextName, boolean firstBuild, String customBuildDate) {
        this.name = name;
        this.appPath = appPath;
        this.nextName = nextName;
        this.firstBuild = firstBuild;
        this.customBuildDate = customBuildDate;
    }

    public String getName() {
        return name;
    }

    public String getAppPath() {
        return appPath;
    }

    public String getNextName() {
        return nextName;
    }

    public boolean getFirstBuild() {
        return firstBuild;
    }

    public String getCustomBuildDate() {
        return customBuildDate;
    }

    @Override
    public String toString() {
        return "WfEntity{" +
                "name='" + name + '\'' +
                ", appPath='" + appPath + '\'' +
                ", nextName='" + nextName + '\'' +
                ", firstBuild=" + firstBuild +
                ", customBuildDate='" + customBuildDate + '\'' +
                '}';
    }
}
