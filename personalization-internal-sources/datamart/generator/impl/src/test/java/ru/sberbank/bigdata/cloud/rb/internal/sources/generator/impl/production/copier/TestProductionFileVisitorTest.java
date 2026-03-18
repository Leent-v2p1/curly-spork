package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.production.copier;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class TestProductionFileVisitorTest {

    private final Path rootDir = Paths.get("test_tmp");
    private final Path testSourceDir = rootDir.resolve("source");
    private final Path testTargetDir = rootDir.resolve("target");
    private TestProductionFileVisitor visitor;
    private SubWfTransformer subWfTransformer;
    private XmlReaderWriter xmlIO;

    @BeforeEach
    void setUp() {
        this.subWfTransformer = mock(SubWfTransformer.class);
        this.xmlIO = mock(XmlReaderWriter.class);
        this.visitor = new TestProductionFileVisitor(testTargetDir, testSourceDir, subWfTransformer, xmlIO);
    }

    @Test
    void visitWorkflowFile() {
        Path testXmlFile = testSourceDir.resolve("workflow.xml");
        visitor.visitFile(testXmlFile, null);

        verify(subWfTransformer).replacePaths(any());
        verify(xmlIO).readXml(any());
        verify(xmlIO).writeXmlToTargetDir(any(), any());
    }

    @Test
    void visitXmlFile() throws IOException {
        Path resource = Paths.get(System.getProperty("user.dir"), "/src/test/resources/generators/path_replacer/test.xml");
        Path sourceXmlFile = testSourceDir.resolve("test.xml");
        Path targetXmlFile = testTargetDir.resolve("test.xml");

        testSourceDir.toFile().mkdirs();
        Files.copy(resource, sourceXmlFile, REPLACE_EXISTING);
        visitor.visitFile(sourceXmlFile, null);

        assertTrue(Files.exists(targetXmlFile));

        FileUtils.deleteDirectory(rootDir.toFile());
    }
}