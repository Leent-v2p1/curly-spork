package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.production.copier;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.CommonDirs.*;

/**
 * Класс осуществляет копирование потоков(workflow.xml) из папки <code>temp-release-workflows/oozie/...</code>
 * в папку <code>release/oozie-app/wf/custom/rb/test-limit</code>, при этом измененяя их соответсвующим образом
 * для формирования тестовых потоков.
 */
public class TestProductionCreator {
    private final Path targetDir;
    private final Path releaseDirFromMain;

    public TestProductionCreator(Path projectBuildDirectory) {
        this.targetDir = projectBuildDirectory.resolve("oozie").resolve(SRC_TEST_PRODUCTION_DIR);
        this.releaseDirFromMain = projectBuildDirectory.resolve(FULL_TEMP_PRODUCTION_DIR);
    }

    public void run(Environment environment) throws IOException {
        SubWfTransformer subWfTransformer = new SubWfTransformer(TEST_WORKFLOWS, TEST_UTILS_DIR, TEST_PARENT_APP_PATH, environment);
        XmlReaderWriter xmlReaderWriter = new XmlReaderWriter();
        TestProductionFileVisitor visitor = new TestProductionFileVisitor(targetDir, releaseDirFromMain, subWfTransformer, xmlReaderWriter);
        Files.walkFileTree(releaseDirFromMain, visitor);
    }
}
