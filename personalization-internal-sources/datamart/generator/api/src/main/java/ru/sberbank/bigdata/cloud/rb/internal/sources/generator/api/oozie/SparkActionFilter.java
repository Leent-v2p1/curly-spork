package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.base.WorkflowType;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.VisibleForTesting;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.ActionConf;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans.Action;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans.ActionImpl;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans.Branch;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans.Fork;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SparkActionFilter {

    private final Map<String, ? extends ActionConf> properties;

    public SparkActionFilter(Map<String, ? extends ActionConf> properties) {
        this.properties = properties;
    }

    @VisibleForTesting
    protected List<Action> getAllLeaf(List<Action> actionList) {
        List<Action> result = new ArrayList<>();
        for (Action action : actionList) {
            result.addAll(getBranch(action));
        }
        return result;
    }

    @VisibleForTesting
    protected List<Action> getBranch(Action action) {
        if (action instanceof ActionImpl) {
            return Collections.singletonList(action);
        } else if (action instanceof Fork) {
            List<Action> result = new ArrayList<>();
            for (Branch branch : ((Fork) action).branches()) {
                for (Action branchAction : branch.actions) {
                    result.addAll(getBranch(branchAction));
                }
            }
            return result;
        } else {
            throw new IllegalArgumentException(action.getClass().getName());
        }
    }

    @VisibleForTesting
    protected WorkflowType getWfType(String actionName) {
        final ActionConf actionConf = properties.get(actionName);
        return WorkflowType.valueOfByKey(actionConf.getType());
    }

    public List<Action> get(List<Action> actionList) {
        return getAllLeaf(actionList).stream()
                .filter(action -> {
                    WorkflowType wfType = getWfType(action.name());
                    return !WorkflowType.SUB_WORKFLOW.equals(wfType) && !WorkflowType.PROPERTIES.equals(wfType);
                })
                .collect(Collectors.toList());
    }
}
