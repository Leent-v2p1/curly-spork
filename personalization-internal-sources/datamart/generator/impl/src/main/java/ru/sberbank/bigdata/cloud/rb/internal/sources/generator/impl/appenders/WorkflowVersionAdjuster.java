package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.appenders;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.SysPropertyTool;

import java.io.IOException;
import java.nio.file.*;
import java.util.stream.Stream;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.functions.SneakyThrowConsumer.sneakyThrow;

public class WorkflowVersionAdjuster {

    private static final Logger log = LoggerFactory.getLogger(WorkflowVersionAdjuster.class);

    private static Stream<Path> searchForWorkflows() throws IOException {
        Path target = Paths.get(SysPropertyTool.safeSystemProperty("build.dir"));
        final PathMatcher pathMatcher = FileSystems.getDefault().getPathMatcher("regex:.*oozie.*");
        return Files.find(target, Integer.MAX_VALUE,
                (path, attributes) -> attributes.isRegularFile()
                        && pathMatcher.matches(path)
                        && path.endsWith("workflow.xml"));
    }

    public static void main(String[] args) throws IOException {
        final QuotesAppender appender = new QuotesAppender();

        Stream<Path> pathStream = searchForWorkflows();
        pathStream.forEach(sneakyThrow(path -> {
            log.info("Trying to add quotes in file {}", path);
            String adjustedXml = appender.appendQuotes(path);
            Files.write(path, adjustedXml.getBytes());
        }));
    }
}
