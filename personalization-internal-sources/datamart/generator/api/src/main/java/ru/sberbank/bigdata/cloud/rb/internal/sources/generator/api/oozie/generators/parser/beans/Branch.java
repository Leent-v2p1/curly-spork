package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import java.util.List;

public class Branch {
    public final List<Action> actions;

    public Branch(List<Action> actions) {
        this.actions = actions;
    }

    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }
}
