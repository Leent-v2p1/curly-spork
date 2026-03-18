package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.copier;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.SchemaAlias;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.service.catalog.Catalog;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.util.FileUtils;

import java.io.IOException;
import java.nio.file.Path;


public class DefaultBuildFileCopier {

    public static void copy(String buildTimestamp, Path projectBaseDir) throws IOException {
        final Path projectBuildDirectory = projectBaseDir.resolve("target");
        final Path oozieDir = projectBuildDirectory.resolve("oozie");
        FileUtils.copyDirectory(projectBaseDir.resolve("src/main/resources/oozie"), oozieDir);

        final String stageFolder = "oozie/develop";
        final String baseAppPath = "oozie-app/masspers";
        copyCatalogs(projectBuildDirectory, projectBuildDirectory.resolve(stageFolder).resolve(baseAppPath));

        FileUtils.copyDirectory(
                projectBuildDirectory.resolve("dqcheck"),
                oozieDir.resolve("production/oozie-app/wf/custom/rb/production/utils/").resolve("dqcheck"));
        FileUtils.copyFile(
                projectBuildDirectory.resolve("datamart-properties.yaml"),
                projectBuildDirectory.resolve(stageFolder)
                        .resolve(baseAppPath)
                        .resolve("common/create-datamart")
                        .resolve("datamart-properties.yaml"));
        FileUtils.copyFile(
                projectBuildDirectory.resolve("jars").resolve("personalization.jar"),
                projectBuildDirectory.resolve(stageFolder)
                        .resolve(baseAppPath)
                        .resolve("common/create-datamart")
                        .resolve("personalization.jar"));
        FileUtils.copyFile(
                projectBuildDirectory.resolve("logging").resolve("custom-log4j.properties"),
                projectBuildDirectory.resolve(stageFolder).resolve(baseAppPath).resolve("common").resolve("custom-log4j.properties"));
        final Path distributionDir = projectBuildDirectory.resolve("distribution_" + buildTimestamp);
        FileUtils.copyFile(
                projectBuildDirectory.resolve("jars").resolve("personalization.jar"),
                distributionDir.resolve("ctl-scheduler").resolve("personalization.jar"));
        FileUtils.copyDirectory(
                projectBaseDir.resolve("src").resolve("main").resolve("resources").resolve("shell-script"),
                distributionDir.resolve("shell-script"));
        FileUtils.copyFile(
                projectBaseDir.resolve("src").resolve("main").resolve("resources").resolve("ctl_client_1.17.4").resolve("ctl_client"),
                distributionDir.resolve("ctl_client_1.17.4").resolve("ctl_client"));
        FileUtils.copyFile(
                projectBaseDir.resolve("src").resolve("main").resolve("resources").resolve("ctl_client_1.0.80").resolve("ctl-client-1.0.80.jar"),
                distributionDir.resolve("ctl_client_1.0.80").resolve("ctl-client-1.0.80.jar"));
    }

    private static void copyCatalogs(Path projectBuildDirectory, Path developDistribution) throws IOException {
        for (String catalogSchema : Catalog.getCatalogSchemas()) {
            FileUtils.copyDirectory(
                    projectBuildDirectory.resolve("catalogs").resolve(catalogSchema),
                    developDistribution.resolve(SchemaAlias.of(catalogSchema).schemaAliasValue).resolve("catalogs")
            );
        }
    }
}
