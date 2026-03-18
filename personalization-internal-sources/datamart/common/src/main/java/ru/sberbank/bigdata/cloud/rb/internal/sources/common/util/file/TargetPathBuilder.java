package ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.file;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;

import java.nio.file.Path;
import java.nio.file.Paths;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.SysPropertyTool.getSystemProperty;

public class TargetPathBuilder extends PathBuilder {

    private final String buildDir;

    public TargetPathBuilder(Environment environment) {
        super(environment);
        this.buildDir = Paths.get(getSystemProperty("build.dir")).toString();
    }

    /**
     * Путь до worklfow реплик на день одной витрины, в папке target.
     * Пример сгенерированного пути для Environment.uat_test:
     * C:\\user_name\personalization-internal-sources\datamart\release\target\oozie\\uat\oozie-app\wf\custom\rb\\uat_test\way4\replica_recovery\test_table
     */
    public Path resolveReplicaOnDatePath(FullTableName datamart) {
        checkBuildDirIsPresent();
        return resolveAppBuildDir()
                .resolve(getSchemaAlias(datamart))
                .resolve(REPLICA_RECOVERY_DIR)
                .resolve(datamart.tableName());
    }

    /**
     * Путь до workflow, в папке target
     * Пример сгенерированного пути для Environment.uat:
     * C:\\user_name\personalization-internal-sources\datamart\release\target\oozie\\uat\oozie-app\wf\custom\rb\\uat\folder
     */
    public Path resolveUatWorkflow(String folderName) {
        return resolveAppBuildDir()
                .resolve(folderName);
    }

    /**
     * Путь до worklfow сгенерированного потока каждой витрины, в папке target.
     * Пример сгенерированного пути для Environment.production_test:
     * C:\\user_name\personalization-internal-sources\datamart\release\target\oozie\production\oozie-app\wf\custom\rb\production_test\generated\way4\create-test-table-wf
     */
    public Path resolveGeneratedWorkflowBuildDir(FullTableName datamart) {
        checkBuildDirIsPresent();
        Path dirInTarget = resolveTargetDir();
        return resolveGeneratedWorkflow(dirInTarget, datamart);
    }

    private Path resolveTargetDir() {
        return Paths.get(buildDir)
                .resolve(OOZIE_DIR)
                .resolve(envPathResolver.resolveDirInTarget());
    }

    private Path resolveAppBuildDir() {
        return resolveTargetDir()
                .resolve(envPathResolver.resolveBaseAppPathFromOozieApp().toString());
    }

    private void checkBuildDirIsPresent() {
        if (buildDir == null || buildDir.isEmpty()) {
            throw new IllegalStateException("build.dir system property isn't specified");
        }
    }
}
