package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.SysPropertyTool;

import java.nio.file.Path;
import java.nio.file.Paths;

public class BuildProperties {
    public final Path projectBaseDir;
    public final Path projectBuildDirectory;
    public final String buildTimestamp;

    public BuildProperties() {
        this.projectBaseDir = Paths.get(SysPropertyTool.safeSystemProperty("project.basedir"));
        this.buildTimestamp = SysPropertyTool.safeSystemProperty("build.timestamp");
        this.projectBuildDirectory = projectBaseDir.resolve("target");
    }

    public void setBuildDirSysProperty() {
        System.setProperty("build.dir", projectBuildDirectory.toString());
    }
}
