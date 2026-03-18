package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.appenders;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.System.lineSeparator;

/**
 * Adding quotes to some of the generated workflows.xml to make them compatible with cloudera 5.14 version
 */

public class QuotesAppender {
    private static final String JAVA_OPTION_REGEX = "spark.*\\s+(-D.*\\s*)*\\r?$";
    private static final Pattern PATTERN = Pattern.compile(JAVA_OPTION_REGEX, Pattern.MULTILINE);

    public String appendQuotes(Path pathToWorkflow) {
        String xml;
        try (Stream<String> linesStream = Files.lines(pathToWorkflow)) {
            xml = linesStream.collect(Collectors.joining(lineSeparator()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        Matcher matcher = PATTERN.matcher(xml);
        StringBuffer buffer = new StringBuffer();
        while (matcher.find()) {
            matcher.appendReplacement(buffer, "\"" + "$0" + "\"");
        }

        matcher.appendTail(buffer);
        return buffer.toString();
    }
}
