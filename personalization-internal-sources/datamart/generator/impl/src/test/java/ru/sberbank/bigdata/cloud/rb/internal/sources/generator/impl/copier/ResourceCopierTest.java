package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.copier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.SchemaAlias;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.service.catalog.Catalog;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ResourceCopierTest {

    public static final String TIMESTAMP = "2017-04-22";
    public static final String STAGE_FOLDER = "stage";
    public static final String BASE_APP_PATH = "base";

    @TempDir Path baseDir;
    private Path projectBuildDirectory;

    @BeforeEach
    void setUp() throws IOException {
        projectBuildDirectory = Files.createDirectory(baseDir.resolve("target"));

        final Path jarsDirectory = Files.createDirectory(projectBuildDirectory.resolve("jars"));
        final Path oozieDirectory = Files.createDirectory(projectBuildDirectory.resolve("oozie"));
        final Path oozieProdDirectory = Files.createDirectory(oozieDirectory.resolve("production"));
        final Path loggingDirectory = Files.createDirectory(projectBuildDirectory.resolve("logging"));
        final Path shellDirectory = Files.createDirectory(projectBuildDirectory.resolve("shell"));
        final Path catalogsDirectory = Files.createDirectory(projectBuildDirectory.resolve("catalogs"));
        final Path stageDirectory = Files.createDirectory(projectBuildDirectory.resolve(STAGE_FOLDER));

        Files.createDirectory(stageDirectory.resolve(BASE_APP_PATH));
        Files.createDirectory(baseDir.resolve("install_files"));
        Files.createDirectory(oozieDirectory.resolve("entity"));
        Files.createDirectory(oozieProdDirectory.resolve("wf"));
        Files.createDirectory(baseDir.resolve("distribution_" + TIMESTAMP));
        Files.createFile(projectBuildDirectory.resolve("datamart-properties.yaml"));
        Files.createFile(jarsDirectory.resolve("personalization.jar"));
        Files.createFile(loggingDirectory.resolve("custom-log4j.properties"));
        Files.createFile(shellDirectory.resolve("fill-properties.sh"));

        for (String catalog : Catalog.getCatalogSchemas()){
           Files.createDirectory(catalogsDirectory.resolve(catalog));
        }

        for (String schema : SchemaAlias.getAllSchemaAliases()){
            Files.createFile(jarsDirectory.resolve("etl-" + schema.replace("_", "-") + ".jar"));
        }
    }

    @Test
    void copy() throws IOException {
        final Path distribDir = projectBuildDirectory.resolve("distribution_" + TIMESTAMP);
        final Path baseDistribDir = distribDir.resolve(BASE_APP_PATH);
        final Path commonDistribDir = baseDistribDir.resolve("utils/common");

        assertExistOrNot(distribDir, baseDistribDir, commonDistribDir, false);

        ResourceCopier.copy(TIMESTAMP, baseDir, STAGE_FOLDER, BASE_APP_PATH);

        assertExistOrNot(distribDir, baseDistribDir, commonDistribDir, true);
    }

    private void assertExistOrNot(Path distribDir, Path baseDistribDir, Path commonDistribDir, boolean shouldExist) {
        assertEquals(Files.exists(commonDistribDir.resolve("datamart-properties.yaml")), shouldExist);
        assertEquals(Files.exists(commonDistribDir.resolve("jars").resolve("personalization.jar")), shouldExist);
        assertEquals(Files.exists(commonDistribDir.resolve("custom-log4j.properties")), shouldExist);
        assertEquals(Files.exists(commonDistribDir.resolve("fill-properties.sh")), shouldExist);
        assertEquals(Files.exists(distribDir.resolve("entity")), shouldExist);
        assertEquals(Files.exists(distribDir.resolve("wf")), shouldExist);

        for (String catalog : Catalog.getCatalogSchemas()) {
            assertEquals(Files.exists(baseDistribDir.resolve(SchemaAlias.of(catalog).schemaAliasValue).resolve("catalogs")), shouldExist);
        }
        for (String schema : SchemaAlias.getAllSchemaAliases()) {
            assertEquals(Files.exists(baseDistribDir.resolve(schema).resolve(schema + "-datamart.jar")), shouldExist);
        }
    }
}