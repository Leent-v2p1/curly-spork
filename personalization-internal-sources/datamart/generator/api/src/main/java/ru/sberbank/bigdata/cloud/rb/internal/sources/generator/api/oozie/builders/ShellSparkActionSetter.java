package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.builders;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.BaseProperties;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.source.shell_action.ACTION;

import java.util.ArrayList;
import java.util.List;

public abstract class ShellSparkActionSetter {
    private final Environment env;
    protected final BaseProperties properties = new BaseProperties();

    public ShellSparkActionSetter(Environment env) {
        this.env = env;
    }

    public void setAction(ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.source.workflow.ACTION action,
                          String actionName) {
        ACTION shell = new ACTION();

        String shellName = getShellName(actionName);
        shell.setNameNode("${nameNode}");
        shell.setJobTracker("${jobTracker}");
        shell.setExec(shellName);

        updateArguments(shell.getArgument(), actionName);

        List<String> files = shell.getFile();
        String shellFile = getShellFile(shellName);
        files.add(shellFile);
        files.addAll(getCommonFiles());
        files.addAll(getAdditionalFiles(actionName));

        action.setShell(shell);
    }

    protected String getShellFile(String shellName) {
        return String.format("${wf:appPath()}/%s#%s", shellName, shellName);
    }

    protected void updateArguments(List<String> arguments, String actionName) {

    }

    protected abstract String getShellName(String actionName);

    protected List<String> getCommonFiles() {
        List<String> result = new ArrayList<>();
        String jarPath = properties.getJarPathForEnv(env);
        String logPath = properties.getLogPathForEnv(env);
        String gpPath = properties.getGpPathForEnv(env);
        String jaasPath = properties.getJaasPathForEnv(env);
        String pgPath = properties.getPgPathForEnv(env);
        String keystorePath = properties.getKeystorePathForEnv(env);
        String logName = String.format("%s#%s", logPath, "custom-log4j.properties");
        result.add(jarPath);
        result.add(gpPath);
        result.add(jaasPath);
        result.add(pgPath);
        result.add(keystorePath);
        result.add("${keytabPath}#${replaceAll(keytabPath, '/keytab/', '')}");
        result.add(logName);
        return result;
    }

    abstract List<String> getAdditionalFiles(String actionName);

    protected Environment getEnv() {
        return env;
    }
}
