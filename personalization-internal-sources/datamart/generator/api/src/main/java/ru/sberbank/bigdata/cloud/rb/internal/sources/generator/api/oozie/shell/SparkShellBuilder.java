package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.shell;

import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.WorkflowNameEscaper;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.ActionConf;

import java.util.HashMap;
import java.util.Map;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.FreemarkerHelper.getTemplate;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.FreemarkerHelper.printTemplate;

public class SparkShellBuilder<T extends ActionConf> {

    protected final String actionName;
    protected final T actionConf;

    public SparkShellBuilder(String actionName, T actionConf) {
        this.actionName = actionName;
        this.actionConf = actionConf;
    }

    public String buildShell() {
        return printTemplate(getTemplate("spark-shell.ftl"), getParams());
    }

    private Map<String, Object> getParams() {
        Map<String, Object> params = new HashMap<>();
        params.put("className", actionConf.getClassName());
        params.put("datamart", WorkflowNameEscaper.escape(actionName));
        params.put("jarName", actionConf.getJarName());
        params.put("extraJarNames", actionConf.getExtraJarNames());
        return params;
    }
}
