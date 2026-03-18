package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import java.util.List;

public class Fork implements Action {
    private final String name;
    private final List<Branch> branches;

    public Fork(String name, List<Branch> branches) {
        this.name = name;
        this.branches = branches;
    }

    @Override
    public String name() {
        return name;
    }

    public List<Branch> branches() {
        return branches;
    }

    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }
}
