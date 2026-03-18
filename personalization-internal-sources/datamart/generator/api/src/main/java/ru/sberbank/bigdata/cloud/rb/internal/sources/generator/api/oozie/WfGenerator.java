package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie;

import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.builders.actions.ActionParamsBuilder;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.source.components.properties_generators.StringConstants;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.source.workflow.*;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans.Action;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans.ActionImpl;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans.Branch;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans.Fork;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

/**
 * Генерирует экземпляр класса WORKFLOWAPP из листа Action
 */
public class WfGenerator {
    private final ActionTypeMapper actionTypeMapper;

    public WfGenerator(ActionTypeMapper actionTypeMapper) {
        this.actionTypeMapper = actionTypeMapper;
    }

    public WORKFLOWAPP generate(List<Action> wfs) {
        return generate(wfs, last(wfs).name(), head(wfs).name());
    }

    public WORKFLOWAPP generate(List<Action> wfs, String workflowAppName) {
        return generate(wfs, workflowAppName, head(wfs).name());
    }

    public WORKFLOWAPP generate(List<Action> wfs, String workflowAppName, String startActionName) {
        WORKFLOWAPP workflowapp = new WORKFLOWAPP();
        workflowapp.setName(escapeName(workflowAppName));
        workflowapp.setGlobal(createGlobal());
        workflowapp.setStart(createStart(escapeName(startActionName)));
        workflowapp.getDecisionOrForkOrJoin().addAll(createDecisionOrForkOrJoin(wfs));
        workflowapp.getDecisionOrForkOrJoin().add(createKill());
        workflowapp.setEnd(createEnd());
        return workflowapp;
    }

    private GLOBAL createGlobal() {
        GLOBAL global = new GLOBAL();
        CONFIGURATION configuration = new CONFIGURATION();
        List<CONFIGURATION.Property> globalProperties = configuration.getProperty();

        CONFIGURATION.Property queue = new CONFIGURATION.Property();
        queue.setName(StringConstants.MAPRED_JOB_QUEUE_NAME.getValue());
        queue.setValue(StringConstants.YARN_QUEUE_VALUE.getValue());

        CONFIGURATION.Property parentAppPath = new CONFIGURATION.Property();
        parentAppPath.setName(StringConstants.PARENT_APP_PATH.getValue());
        parentAppPath.setValue(StringConstants.WF_APP_PATH_VALUE.getValue());

        CONFIGURATION.Property maxAttempts = new CONFIGURATION.Property();
        maxAttempts.setName(StringConstants.YARN_MAX_ATTEMPTS.getValue());
        maxAttempts.setValue("1");

        globalProperties.add(queue);
        globalProperties.add(parentAppPath);
        globalProperties.add(maxAttempts);
        global.setConfiguration(configuration);
        return global;
    }

    private START createStart(String startActionName) {
        START start = new START();
        start.setTo(startActionName);
        return start;
    }

    private List<Object> createDecisionOrForkOrJoin(List<Action> wfs) {
        return decisionOrForkOrJoin(wfs, transitions("end", "kill"), "kill");
    }

    private Tuple2<ACTIONTRANSITION, ACTIONTRANSITION> transitions(String ok, String error) {
        ACTIONTRANSITION okTransition = new ACTIONTRANSITION();
        okTransition.setTo(escapeName(ok));
        ACTIONTRANSITION killTransition = new ACTIONTRANSITION();
        killTransition.setTo(escapeName(error));
        return new Tuple2<>(okTransition, killTransition);
    }

    private List<Object> decisionOrForkOrJoin(List<Action> wfs,
                                              Tuple2<ACTIONTRANSITION, ACTIONTRANSITION> lastTransitions,
                                              String errorActionName) {
        List<Object> decisionOrForkOrJoin = new ArrayList<>();
        for (int i = 0; i < wfs.size(); i++) {
            Tuple2<ACTIONTRANSITION, ACTIONTRANSITION> transitions;
            if (i == wfs.size() - 1) { //if last
                transitions = lastTransitions;
            } else {
                final Action nextAction = wfs.get(i + 1);
                transitions = transitions(nextAction.name(), errorActionName);
            }

            final Action action = wfs.get(i);
            final List<Object> subDecisionOrForkJoin = route(action, transitions);
            decisionOrForkOrJoin.addAll(subDecisionOrForkJoin);
        }
        return decisionOrForkOrJoin;
    }

    private List<Object> route(Action action, Tuple2<ACTIONTRANSITION, ACTIONTRANSITION> transitions) {
        if (action instanceof ActionImpl) {
            return singletonList(decisionOrForkOrJoin(((ActionImpl) action), transitions));
        } else if (action instanceof Fork) {
            return decisionOrForkOrJoin(((Fork) action), transitions);
        } else {
            throw new IllegalStateException();
        }
    }

    private ACTION decisionOrForkOrJoin(ActionImpl actionImpl, Tuple2<ACTIONTRANSITION, ACTIONTRANSITION> nextTransitions) {
        ActionParamsBuilder actionParamsBuilder = actionTypeMapper.resolve(actionImpl.name());
        ACTION action = actionParamsBuilder.buildActionParams();

        action.setOk(nextTransitions._1);
        action.setError(nextTransitions._2);
        return action;
    }

    private List<Object> decisionOrForkOrJoin(Fork wfFork, Tuple2<ACTIONTRANSITION, ACTIONTRANSITION> nextTransitions) {
        List<Object> decisionOrForkOrJoin = new ArrayList<>();

        FORK fork = new FORK();
        fork.setName(escapeName(wfFork.name()));
        List<FORKTRANSITION> paths = wfFork.branches().stream()
                .map(branch -> {
                    final String branchName = head(branch.actions).name();
                    final FORKTRANSITION forkTransition = new FORKTRANSITION();
                    forkTransition.setStart(escapeName(branchName));
                    return forkTransition;
                })
                .collect(toList());
        fork.getPath().addAll(paths);
        decisionOrForkOrJoin.add(fork);

        JOIN join = new JOIN();
        join.setName(escapeName(wfFork.name() + "-join"));
        join.setTo(escapeName(wfFork.name() + "-decision"));
        decisionOrForkOrJoin.add(join);

        DECISION decision = new DECISION();
        decision.setName(escapeName(wfFork.name() + "-decision"));
        decision.setSwitch(createSwitch(nextTransitions._1.getTo()));
        decisionOrForkOrJoin.add(decision);

        for (Branch branch : wfFork.branches()) {
            decisionOrForkOrJoin.addAll(decisionOrForkOrJoin(branch.actions, transitions(join.getName(), join.getName()), join.getName()));
        }
        return decisionOrForkOrJoin;
    }

    private SWITCH createSwitch(String okTransition) {
        SWITCH aSwitch = new SWITCH();
        List<CASE> aCase = aSwitch.getCase();
        CASE errorCase = new CASE();
        errorCase.setValue("${not empty wf:lastErrorNode()}");
        errorCase.setTo("kill");
        aCase.add(errorCase);
        DEFAULT aDefault = new DEFAULT();
        aDefault.setTo(escapeName(okTransition));
        aSwitch.setDefault(aDefault);
        return aSwitch;
    }

    private KILL createKill() {
        KILL kill = new KILL();
        kill.setName("kill");
        kill.setMessage(StringConstants.ACTION_FAILED_ERROR_MESSAGE.getValue());
        return kill;
    }

    private END createEnd() {
        END end = new END();
        end.setName("end");
        return end;
    }

    private String escapeName(String name) {
        return WorkflowNameEscaper.escape(name);
    }

    private <T> T head(List<T> list) {
        return list.get(0);
    }

    private <T> T last(List<T> list) {
        return list.get(list.size() - 1);
    }
}
