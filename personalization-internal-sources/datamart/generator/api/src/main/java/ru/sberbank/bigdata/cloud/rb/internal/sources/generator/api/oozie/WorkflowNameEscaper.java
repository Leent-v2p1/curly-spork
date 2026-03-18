package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie;

public class WorkflowNameEscaper {

    /**
     * Escapes symbols (whitespaces, dot, underscore) in identifiers
     */
    public static String escape(String name) {
        return name.replaceAll("[\\s._]", "-");
    }
}
