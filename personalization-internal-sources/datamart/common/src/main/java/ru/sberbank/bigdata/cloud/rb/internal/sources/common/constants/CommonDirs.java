package ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants;

import java.nio.file.Path;
import java.nio.file.Paths;

public class CommonDirs {
    public static final String PRODUCTION_PARENT_APP_PATH = PARENT_APP_PATH + "/production";

    public static final String TEST_BASE_DIR = "/production_test";
    public static final String TEST_PARENT_APP_PATH = PARENT_APP_PATH + TEST_BASE_DIR;
    public static final String SRC_TEST_PRODUCTION_DIR = "production/" + TEST_PARENT_APP_PATH;
    public static final String SRC_PRODUCTION_DIR = "/production/" + PARENT_APP_PATH;
    public static final String TEMP_PRODUCTION_WORKFLOWS = "temp-production-workflows";
    public static final String UTILS_DIR = PRODUCTION_PARENT_APP_PATH + "/utils";
    public static final String TEST_UTILS_DIR = PARENT_APP_PATH + TEST_BASE_DIR + "/utils";
    public static final String FULL_TEMP_PRODUCTION_DIR = TEMP_PRODUCTION_WORKFLOWS + "/oozie" + SRC_PRODUCTION_DIR + "/production";

    public static final String PRODUCTION_WORKFLOWS = PRODUCTION_PARENT_APP_PATH + "/generated";

    public static final String TEST_WORKFLOWS = TEST_PARENT_APP_PATH + "/generated";

    public static final Path BASE_APP_PATH = Paths.get(PARENT_APP_PATH);
    public static final Path PRODUCTION_BASE_APP_PATH = BASE_APP_PATH.resolve("production");
    public static final Path PRODUCTION_TEST_BASE_APP_PATH = Paths.get(TEST_PARENT_APP_PATH);

}
