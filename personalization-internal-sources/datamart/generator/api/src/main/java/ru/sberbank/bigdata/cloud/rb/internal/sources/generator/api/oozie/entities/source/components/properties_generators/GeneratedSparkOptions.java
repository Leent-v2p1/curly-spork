package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.source.components.properties_generators;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.FreemarkerHelper;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.MemoryParams;

import java.util.HashMap;
import java.util.Map;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.FreemarkerHelper.getTemplate;

public class GeneratedSparkOptions {
    private static final String TEMPLATE_NAME = "spark-options.ftl";

    protected final Map<String, Object> params;
    private final MemoryParams memoryParams;

    public GeneratedSparkOptions(MemoryParams memoryParams, Map<String, Object> params) {
        this(memoryParams);
        this.params.putAll(params);
    }

    public GeneratedSparkOptions(MemoryParams memoryParams) {
        params = new HashMap<>();
        this.memoryParams = memoryParams;
        initParams();

    }

    private void initParams() {
        params.putAll(memoryParams.toMap());
    }

    public String createOptionsAccordingToTemplate() {
        String generated = FreemarkerHelper.printTemplate(getTemplate(TEMPLATE_NAME), params);

        return generated + "            ";
    }

}
