package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment;

import java.nio.file.Path;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment.PRODUCTION;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.CommonDirs.PRODUCTION_BASE_APP_PATH;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.CommonDirs.PRODUCTION_TEST_BASE_APP_PATH;

public class EnvironmentPathResolver {
    private static final String GENERATED = "generated";
    private static final String UTILS = "utils";
    private Environment env;

    public EnvironmentPathResolver(Environment env) {
        this.env = env;
    }

    public String resolveDirInTarget() {
        switch (env) {
            case PRODUCTION:
            case PRODUCTION_TEST:
                return PRODUCTION.nameLowerCase();
            default:
                throw new IllegalStateException("Can't resolve dirInTarget for env: " + env);
        }
    }

    public Path resolveBaseAppPathFromOozieApp() {
        switch (env) {
            case PRODUCTION:
                return PRODUCTION_BASE_APP_PATH;
            case PRODUCTION_TEST:
                return PRODUCTION_TEST_BASE_APP_PATH;
            default:
                throw new IllegalStateException("No such environment: '" + env + "'");
        }
    }

    public Path resolveGeneratedAppPath(String schema) {
        return resolveBaseAppPathFromOozieApp().resolve(schema).resolve(GENERATED);
    }

    public Path getUtilsDir() {
        return resolveBaseAppPathFromOozieApp().resolve(UTILS);
    }
}
