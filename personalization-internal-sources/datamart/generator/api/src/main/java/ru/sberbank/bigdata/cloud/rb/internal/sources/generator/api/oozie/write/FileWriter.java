package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.write;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.util.FileUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class FileWriter {

    private static final Logger log = LoggerFactory.getLogger(FileWriter.class);

    public void write(Path fullPath, List<FileContent> shellList) {
        log.info("Writing shell {}", shellList);
        try {
            Files.createDirectories(fullPath);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
        shellList.forEach(shell -> {
            Path resultPath = fullPath.resolve(shell.fileName());
            FileUtils.writeFile(resultPath, shell.content().getBytes());
        });
    }
}
