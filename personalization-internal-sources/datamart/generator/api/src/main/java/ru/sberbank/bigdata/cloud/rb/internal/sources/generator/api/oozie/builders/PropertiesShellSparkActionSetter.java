package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.builders;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.properties.PropertiesNameResolver;

import java.util.Collections;
import java.util.List;

public class PropertiesShellSparkActionSetter extends ShellSparkActionSetter {

    private static final String SHELL_NAME = "fill-properties.sh";

    public PropertiesShellSparkActionSetter(Environment env) {
        super(env);
    }

    @Override
    protected void updateArguments(List<String> arguments, String actionName) {
        arguments.add("${ctl_secure}");
        arguments.add("${loading_id}");
        arguments.add("${wf_id}");
        arguments.add("${loading_start}");
        arguments.add("${principal}");
        arguments.add("${wf:conf('user.name')}");
        arguments.add("${replaceAll(keytabPath, '/keytab/', '')}");
        arguments.add("${yarnQueue}");
        arguments.add("${wf:conf('customBuildDate')}");

        String sparkPropertiesName = PropertiesNameResolver.getSparkPropertiesTemplateName(actionName);
        String systemPropertiesName = PropertiesNameResolver.getSystemPropertiesTemplateName(actionName);
        String sparkPropertiesFileName = String.format("${wf:appPath()}/%s", sparkPropertiesName);
        String systemPropertiesFileName = String.format("${wf:appPath()}/%s", systemPropertiesName);
        arguments.add(sparkPropertiesFileName);
        arguments.add(systemPropertiesFileName);
        arguments.add("${ctlApiVersionV5}");
    }

    @Override
    protected String getShellName(String actionName) {
        return SHELL_NAME;
    }

    @Override
    protected String getShellFile(String shellName) {
        return properties.getPropertiesShellPathForEnv(getEnv());
    }

    @Override
    List<String> getAdditionalFiles(String actionName) {
        return Collections.emptyList();
    }
}
