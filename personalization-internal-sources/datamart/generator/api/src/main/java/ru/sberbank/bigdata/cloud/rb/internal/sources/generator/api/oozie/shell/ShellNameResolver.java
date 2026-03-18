package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.shell;

import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.WorkflowNameEscaper;

public class ShellNameResolver {

    public static String getShellName(String actionName) {
        return WorkflowNameEscaper.escape(actionName) + "-runner.sh";
    }
}
