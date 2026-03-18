package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.production.copier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.file.FileMatchers;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.file.FileMatchers.*;

public class TestProductionFileVisitor extends SimpleFileVisitor<Path> {

    private static final Logger log = LoggerFactory.getLogger(TestProductionFileVisitor.class);
    private final Path targetDir;
    private final Path productionDir;
    private final SubWfTransformer subWfTransformer;
    private final XmlReaderWriter xmlIO;

    public TestProductionFileVisitor(Path targetDir,
                                     Path productionDir,
                                     SubWfTransformer subWfTransformer,
                                     XmlReaderWriter xmlIO) {
        this.targetDir = targetDir;
        this.productionDir = productionDir;
        this.subWfTransformer = subWfTransformer;
        this.xmlIO = xmlIO;
    }

    @Override
    public FileVisitResult visitFile(Path filePath, BasicFileAttributes attrs) {
        if (filePath.startsWith(targetDir) || filePath.toString().contains("generated")) {
            log.warn("Ignoring file = {}", filePath);
            return FileVisitResult.CONTINUE;
        }
        Path fileName = filePath.getFileName();
        Path newPath = transformPath(filePath);
        log.error("old path = {}", filePath);
        log.error("new path = {}", newPath);
        if (IS_WORKFLOW_FILE_MATCHER.matches(fileName)) {
            Document xml = xmlIO.readXml(filePath);
            subWfTransformer.replacePaths(xml);
            subWfTransformer.replaceWfAppName(xml);
            xmlIO.writeXmlToTargetDir(xml, newPath);
        } else if (IS_SQL_FILE_MATCHER.matches(fileName) || IS_SH_FILE_MATCHER.matches(fileName) || IS_PY_FILE_MATCHER.matches(fileName)) {
            try {
                log.error("trying to copy sql, sh and py file {}", filePath);
                newPath.getParent().toFile().mkdirs();
                Files.copy(filePath, newPath, REPLACE_EXISTING);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        } else if (IS_XML_FILE_MATCHER.matches(fileName)) {
            try {
                newPath.getParent().toFile().mkdirs();
                Files.copy(filePath, newPath, REPLACE_EXISTING);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return FileVisitResult.CONTINUE;
    }

    private Path transformPath(Path file) {
        if (FileMatchers.contains(file, productionDir)) {
            return Paths.get(file.toString().replace(productionDir.toString(), targetDir.toString()));
        } else {
            throw new IllegalStateException("File " + file + " should starts from " + productionDir);
        }
    }
}
