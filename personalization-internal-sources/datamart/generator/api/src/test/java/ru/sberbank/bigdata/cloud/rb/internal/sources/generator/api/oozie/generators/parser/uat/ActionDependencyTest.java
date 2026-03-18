package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.uat;

import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.ResultActionConf;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans.Action;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans.Branch;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans.Fork;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ActionDependencyTest {

    @Test
    void noDependencies() {
        final ResultActionConf actionConfig = createActionConfig("name1");
        final List<ResultActionConf> resultActionConfs = singletonList(actionConfig);
        final ActionDependency actionDependency = new ActionDependency(resultActionConfs);

        final List<Action> actionWithDependencies = actionDependency.resolve(actionConfig);

        assertThat(actionWithDependencies, hasSize(1));
        assertEquals("name1", actionWithDependencies.get(0).name());
    }

    @Test
    void singleDependency() {
        final ResultActionConf actionConfig = createActionConfig("dependency1");
        final ResultActionConf actionConfigMain = createActionConfig("main-action", singletonList("dependency1"));
        final List<ResultActionConf> resultActionConfs = Arrays.asList(actionConfig, actionConfigMain);
        final ActionDependency actionDependency = new ActionDependency(resultActionConfs);

        final List<Action> actionWithDependencies = actionDependency.resolve(actionConfigMain);

        assertThat(actionWithDependencies, hasSize(2));
        assertEquals("main-action", actionWithDependencies.get(1).name());
        assertEquals("dependency1", actionWithDependencies.get(0).name());
    }

    @Test
    void forkDependency() {
        final ResultActionConf actionConfig = createActionConfig("dependency1");
        final ResultActionConf actionConfig2 = createActionConfig("dependency2");
        final List<String> branchesList = Arrays.asList(actionConfig.getName(), actionConfig2.getName());
        Map<String, Object> branches = new HashMap<>();
        branches.put("branches", branchesList);
        Map<String, Map<String, Object>> fork = new HashMap<>();
        fork.put("fork", branches);
        final ResultActionConf actionConfigMain = createActionConfig("main-action", singletonList(fork));
        final List<ResultActionConf> resultActionConfs = Arrays.asList(actionConfig, actionConfig2, actionConfigMain);
        final ActionDependency actionDependency = new ActionDependency(resultActionConfs);

        final List<Action> actionWithDependencies = actionDependency.resolve(actionConfigMain);

        assertThat(actionWithDependencies, hasSize(2));
        assertEquals("main-action", actionWithDependencies.get(1).name());
        final Action actualFork = actionWithDependencies.get(0);
        assertTrue(actualFork instanceof Fork);
        final List<Branch> actualBranches = ((Fork) actualFork).branches();
        assertThat(actualBranches, hasSize(2));
        assertThat(actualBranches.get(0).actions, hasSize(1));
        assertEquals("dependency1", actualBranches.get(0).actions.get(0).name());
        assertThat(actualBranches.get(1).actions, hasSize(1));
        assertEquals("dependency2", actualBranches.get(1).actions.get(0).name());
    }

    private ResultActionConf createActionConfig(String actionName) {
        return createActionConfig(actionName, null);
    }

    private ResultActionConf createActionConfig(String actionName, List<Object> dependencies) {
        final ResultActionConf resultActionConf = new ResultActionConf();
        resultActionConf.setName(actionName);
        if (dependencies != null) {
            resultActionConf.setDependencies(dependencies);
        }
        return resultActionConf;
    }
}