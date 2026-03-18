package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.appenders;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;

import static java.lang.System.lineSeparator;
import static org.junit.jupiter.api.Assertions.assertEquals;

class QuotesAppenderTest {
    private static final String PATH_TO_WORKFLOW = "src/test/resources/appenders/sample_workflow.xml";
    private static final String PATH_TO_EXPECTED_WORKFLOW = "src/test/resources/appenders/expected_workflow.xml";

    @Test
    void testQuotesAppending() throws IOException {
        String expectedXml = Files.lines(Paths.get(PATH_TO_EXPECTED_WORKFLOW)).collect(Collectors.joining(lineSeparator()));

        QuotesAppender appender = new QuotesAppender();

        String resultedXml = appender.appendQuotes(Paths.get(PATH_TO_WORKFLOW));
        assertEquals(expectedXml, resultedXml);
    }
}