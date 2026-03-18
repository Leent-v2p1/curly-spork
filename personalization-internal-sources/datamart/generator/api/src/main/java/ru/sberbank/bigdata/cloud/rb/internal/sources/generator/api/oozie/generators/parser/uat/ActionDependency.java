package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.uat;

import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.ActionConf;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans.Action;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans.ActionImpl;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans.Branch;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans.Fork;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class ActionDependency {
    private final List<? extends ActionConf> actionConfs;

    public ActionDependency(List<? extends ActionConf> actionConfs) {
        this.actionConfs = actionConfs;
    }

    public List<Action> resolve(ActionConf action) {
        List<Action> result = new ArrayList<>();
        if (action.getDependencies() != null) {
            result.addAll(resolveDependencies(action));
        }
        result.add(new ActionImpl(action.getName()));
        return result;
    }

    private List<Action> resolveDependencies(ActionConf action) {
        List<Action> result = new ArrayList<>();
        for (Object dependency : action.getDependencies()) {
            result.addAll(resolveDependency(dependency));
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private List<Action> resolveDependency(Object dependency) {
        List<Action> result = new ArrayList<>();
        if (dependency instanceof String) {
            final String dependencyName = (String) dependency;
            final ActionConf found = actionConfs.stream()
                    .filter(actionConf -> actionConf.getName().equals(dependencyName))
                    .findAny()
                    .orElseThrow(() -> new IllegalStateException("Cannot find dependency with name: " + dependencyName));
            result.addAll(requireNonNull(resolve(found)));
        } else if (dependency instanceof Map) {
            final Map<String, Map<String, Object>> forkMap = (Map<String, Map<String, Object>>) dependency;
            final Map<String, Object> fork = forkMap.get("fork");
            final List<Branch> branches = ((List<Object>) fork.get("branches")).stream()
                    .map(this::resolveDependency)
                    .map(Branch::new)
                    .collect(toList());
            result.add(new Fork((String) fork.get("name"), branches));
        }
        return result;
    }
}
