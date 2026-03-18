package ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.file;

import java.nio.file.Path;

public class AppPath {

    public static String unixPath(Path platformDependentPath) {
        String unixPath = platformDependentPath.toString().replaceAll("\\\\", "/");
        if (!unixPath.startsWith("/")) {
            unixPath = "/" + unixPath;
        }
        return unixPath;
    }

    public static String relativeUnixPath(Path platformDependentPath) {
        String unixPath = platformDependentPath.toString().replaceAll("\\\\", "/");
        if (unixPath.startsWith("/")) {
            unixPath = unixPath.replaceAll("^/", "");
        }
        return unixPath;
    }
}
