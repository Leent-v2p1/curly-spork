package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.builders;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.PropertiesUtils;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.file.PathBuilder;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.properties.PropertiesNameResolver;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.shell.ShellNameResolver;

import java.util.ArrayList;
import java.util.List;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.NameAdditions.DATA_QUALITY_CHECK_POSTFIX;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.NameAdditions.RESERVE_POSTFIX;

public class ExecutionShellSparkActionSetter extends ShellSparkActionSetter {

    public ExecutionShellSparkActionSetter(Environment env) {
        super(env);
    }

    @Override
    protected String getShellName(String actionName) {
        return ShellNameResolver.getShellName(actionName);
    }

    @Override
    List<String> getAdditionalFiles(String actionName) {
        List<String> result = new ArrayList<>();
        String sparkPropertiesName = PropertiesNameResolver.getSparkPropertiesName(actionName);
        String systemPropertiesName = PropertiesNameResolver.getSystemPropertiesName(actionName);

        String sparkPropertiesFileName = String.format("${wf:appPath()}/%s#%s", sparkPropertiesName, sparkPropertiesName);
        String systemPropertiesFileName = String.format("${wf:appPath()}/%s#%s", systemPropertiesName, systemPropertiesName);
        result.add(sparkPropertiesFileName);
        result.add(systemPropertiesFileName);
        result.add(new PathBuilder(getEnv()).schedulerPoolXmlPath());

        final FullTableName actionTableName = FullTableName.of(actionName);
        final boolean isDatamartAction = PropertiesUtils.getAllActionsId().contains(actionName);
        if ((actionName.endsWith(RESERVE_POSTFIX) || actionName.endsWith(DATA_QUALITY_CHECK_POSTFIX) || isDatamartAction)) {
            result.add(new PathBuilder(getEnv()).jarPath(actionTableName));
        }

        return result;
    }
}
