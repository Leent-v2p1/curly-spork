package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.copier;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.SchemaAlias;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.service.catalog.Catalog;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.util.FileUtils;

import java.io.IOException;
import java.nio.file.Path;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.SchemaAlias.getAllSchemaAliases;

public class ResourceCopier {

    public static void copy(String buildTimestamp, Path projectBaseDir, String stageFolder, String baseAppPath) throws IOException {
        final Path projectBuildDirectory = projectBaseDir.resolve("target");

        final Path distributionDir = projectBuildDirectory.resolve("distribution_" + buildTimestamp);
        FileUtils.copyFile(projectBuildDirectory.resolve("datamart-properties.yaml"), distributionDir.resolve(baseAppPath)
                .resolve("utils/common")
                .resolve("datamart-properties.yaml"));
        FileUtils.copyFile(projectBuildDirectory.resolve("jars").resolve("personalization.jar"), distributionDir.resolve(baseAppPath)
                .resolve("utils/common/jars")
                .resolve("personalization.jar"));
        FileUtils.copyFile(projectBuildDirectory.resolve("logging").resolve("custom-log4j.properties"), distributionDir.resolve(baseAppPath)
                .resolve("utils/common")
                .resolve("custom-log4j.properties"));
        FileUtils.copyFile(projectBuildDirectory.resolve("shell").resolve("fill-properties.sh"), distributionDir.resolve(baseAppPath)
                .resolve("utils/common")
                .resolve("fill-properties.sh"));
        copyCatalogs(projectBuildDirectory, distributionDir, baseAppPath);
        copyModuleJars(projectBuildDirectory, distributionDir, baseAppPath);

        FileUtils.copyDirectory(projectBuildDirectory.resolve(stageFolder).resolve(baseAppPath), distributionDir.resolve(baseAppPath));

        FileUtils.copyDirectory(projectBaseDir.resolve("install_files"), distributionDir);
        FileUtils.copyDirectory(projectBuildDirectory.resolve("oozie/entity"), distributionDir.resolve("entity"));
        FileUtils.copyDirectory(projectBuildDirectory.resolve("oozie/production/wf"), distributionDir.resolve("wf"));
    }

    private static void copyCatalogs(Path projectBuildDirectory, Path distributionDir, String baseAppPath) throws IOException {
        for (String catalogSchema : Catalog.getCatalogSchemas()) {
            FileUtils.copyDirectory(
                    projectBuildDirectory.resolve("catalogs").resolve(catalogSchema),
                    distributionDir.resolve(baseAppPath).resolve(SchemaAlias.of(catalogSchema).schemaAliasValue).resolve("catalogs")
            );
        }
    }

    private static void copyModuleJars(Path projectBuildDirectory, Path distributionDir, String baseAppPath) throws IOException {
        for (String schema : getAllSchemaAliases()) {
            Path path = projectBuildDirectory.resolve("jars").resolve("etl-" + schema.replace("_", "-") + ".jar");
            if (path.toString().contains("greenplum")) {
                path = projectBuildDirectory.resolve("jars").resolve("etl-" + schema.replace("_", "-") + "-sql.jar");
            }
            System.out.println("Path IS: " + path);
            if (path.toFile().exists()) {
                FileUtils.copyFile(
                        path,
                        distributionDir
                                .resolve(baseAppPath)
                                .resolve(schema)
                                .resolve(schema + "-datamart.jar")
                );
            }
        }
    }
}
