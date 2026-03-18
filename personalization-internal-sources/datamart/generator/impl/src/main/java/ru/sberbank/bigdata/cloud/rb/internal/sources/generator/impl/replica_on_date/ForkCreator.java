package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.replica_on_date;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.NameAdditions;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans.Action;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans.ActionImpl;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans.Branch;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans.Fork;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * генерирует набор Action(ActionImpl и Fork), который составляет из них набор последовательный форков,
 * каждый из которых содержит максимум action указанных в workflowPerFork
 */
public class ForkCreator {

    public List<Action> createForkWithMaxSize(List<String> workflows, int workflowPerFork) {
        if (workflowPerFork < 2) {
            throw new IllegalArgumentException("fork should contain at least 2 actions");
        }

        int size = workflows.size();
        int forks = size / workflowPerFork;
        List<Action> actionList = new ArrayList<>(forks);
        for (int i = 0; i < forks; i++) {
            actionList.addAll(createAction(i, workflows.subList(workflowPerFork * i, workflowPerFork * (i + 1))));
        }
        if (workflowPerFork * forks != size) {
            actionList.addAll(createAction(forks, workflows.subList(workflowPerFork * forks, workflows.size())));
        }
        return actionList;
    }

    private List<Action> createAction(int forkNumber, List<String> subListForFork) {
        if (subListForFork.size() == 1) {
            Action action = new ActionImpl(subListForFork.get(0));
            Action propertiesAction = createPropertiesAction(action);
            return Arrays.asList(propertiesAction, action);
        } else {
            List<Branch> collect = subListForFork.stream()
                    .map(ActionImpl::new)
                    .map(action -> {
                        Action actionProperties = createPropertiesAction(action);
                        return new Branch(Arrays.asList(actionProperties, action));
                    })
                    .collect(Collectors.toList());
            return Collections.singletonList(new Fork("fork-" + forkNumber, collect));
        }
    }

    private Action createPropertiesAction(Action action) {
        return new ActionImpl(action.name() + NameAdditions.PROPERTIES_POSTFIX);
    }
}
