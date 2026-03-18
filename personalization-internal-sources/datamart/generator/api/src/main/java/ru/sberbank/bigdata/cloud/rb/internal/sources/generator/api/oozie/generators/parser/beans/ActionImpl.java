package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

public class ActionImpl implements Action {
    private final String name;

    public ActionImpl(String name) {
        this.name = name;
    }

    @Override
    public String name() {
        return name;
    }

    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }
}
