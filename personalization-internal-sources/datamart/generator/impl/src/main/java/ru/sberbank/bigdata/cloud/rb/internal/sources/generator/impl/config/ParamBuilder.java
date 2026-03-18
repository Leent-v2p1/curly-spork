package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.config;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.functions.CollectionUtils;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.config.param_setter.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ParamBuilder {

    private SparkDefaultSetter sparkDefaultSetter;
    private CommonSetter commonSetter;
    private ExecutionSetter executionSetter;
    private CustomSetter customSetter;

    public static ParamBuilder builder() {
        return new ParamBuilder();
    }

    public ParamBuilder withSparkDefault() {
        this.sparkDefaultSetter = new SparkDefaultSetter();
        return this;
    }

    public ParamBuilder withCommon(Map<String, String> wfParam) {
        this.commonSetter = new CommonSetter(wfParam);
        return this;
    }

    public ParamBuilder withExecute(Map<String, String> wfParam, String tableName) {
        this.executionSetter = new ExecutionSetter(wfParam, tableName);
        return this;
    }

    public ParamBuilder withCustom(Map<String, String> wfParam, String tableName) {
        this.customSetter = new CustomSetter(wfParam, tableName);
        return this;
    }

    public List<ParamSetter> result() {
        List<ParamSetter> setters = new ArrayList<>();
        //order of setters is important
        CollectionUtils.addIfNotNull(setters, sparkDefaultSetter);
        CollectionUtils.addIfNotNull(setters, commonSetter);
        CollectionUtils.addIfNotNull(setters, executionSetter);
        CollectionUtils.addIfNotNull(setters, customSetter);
        return setters;
    }
}
