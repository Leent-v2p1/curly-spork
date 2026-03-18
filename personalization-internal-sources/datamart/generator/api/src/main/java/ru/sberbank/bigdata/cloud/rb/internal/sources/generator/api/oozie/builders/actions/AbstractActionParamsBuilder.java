package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.builders.actions;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;

public abstract class AbstractActionParamsBuilder<T> implements ActionParamsBuilder {
    protected final String actionName;
    protected final T actionConf;
    protected final Environment env;

    AbstractActionParamsBuilder(String actionName, T actionConf, Environment env) {
        this.actionName = actionName;
        this.actionConf = actionConf;
        this.env = env;
    }
}
