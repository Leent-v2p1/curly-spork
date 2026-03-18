package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class FileUtils {
    private static final Logger log = LoggerFactory.getLogger(FileUtils.class);

    private FileUtils() {
        throw new IllegalStateException("Utility class");
    }

    public static void copyDirectory(Path src, Path destDir) throws IOException {
        log.info("Copying directory: srcDir = {}, destDir = {}", src, destDir);
        org.apache.commons.io.FileUtils.copyDirectory(src.toFile(), destDir.toFile());
    }

    public static void copyFile(Path src, Path dest) throws IOException {
        log.info("Copying file: src = {}, dest = {}", src, dest);
        org.apache.commons.io.FileUtils.copyFile(src.toFile(), dest.toFile());
    }

    public static void writeFile(Path path, byte[] content) {
        log.info("Writing file: {}", path);
        try {
            Files.write(path, content, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static String readAsString(Path path) {
        log.info("Reading file: {}", path);
        try {
            byte[] encoded = Files.readAllBytes(path);
            return new String(encoded, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
