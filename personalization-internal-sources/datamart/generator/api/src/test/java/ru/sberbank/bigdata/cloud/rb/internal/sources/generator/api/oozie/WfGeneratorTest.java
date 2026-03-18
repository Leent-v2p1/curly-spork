package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie;

import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.builders.actions.ActionParamsBuilder;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.source.workflow.*;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans.ActionImpl;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans.Branch;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans.Fork;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class WfGeneratorTest {

    @Test
    void testGenerate() {
        final Branch branch1 = new Branch(singletonList(new ActionImpl("stg_1")));
        final Branch branch2 = new Branch(asList(new ActionImpl("stg_2"), new ActionImpl("stg_3")));
        final Fork fork = new Fork("fork_1", asList(branch1, branch2));
        final ActionTypeMapperImpl actionTypeMapperMock = mock(ActionTypeMapperImpl.class);
        when(actionTypeMapperMock.resolve(anyString())).then(i -> {
            final ACTION action = new ACTION();
            action.setName(WorkflowNameEscaper.escape((String) i.getArguments()[0]));
            ActionParamsBuilder mock = mock(ActionParamsBuilder.class);
            when(mock.buildActionParams()).thenReturn(action);
            return mock;
        });
        final WORKFLOWAPP workflowapp = new WfGenerator(actionTypeMapperMock).generate(asList(new ActionImpl("action_1"), fork, new ActionImpl("action_2")));

        assertEquals("action-2", workflowapp.getName());
        assertEquals("action-1", workflowapp.getStart().getTo());
        assertEquals("end", workflowapp.getEnd().getName());

        assertThat(workflowapp.getGlobal().getConfiguration().getProperty(), hasSize(3));
        assertEquals("mapred.job.queue.name", workflowapp.getGlobal().getConfiguration().getProperty().get(0).getName());
        assertEquals("${yarnQueue}", workflowapp.getGlobal().getConfiguration().getProperty().get(0).getValue());
        assertEquals("parentAppPath", workflowapp.getGlobal().getConfiguration().getProperty().get(1).getName());
        assertEquals("${wf:appPath()}", workflowapp.getGlobal().getConfiguration().getProperty().get(1).getValue());
        assertEquals("yarn.resourcemanager.am.max-attempts", workflowapp.getGlobal().getConfiguration().getProperty().get(2).getName());
        assertEquals("1", workflowapp.getGlobal().getConfiguration().getProperty().get(2).getValue());

        final List<Object> decisionOrForkOrJoin = workflowapp.getDecisionOrForkOrJoin();
        assertThat(decisionOrForkOrJoin, hasSize(9));
        assertAction(decisionOrForkOrJoin, 0, "action-1", "fork-1", "kill");
        assertForkJoin(decisionOrForkOrJoin);
        assertAction(decisionOrForkOrJoin, 4, "stg-1", "fork-1-join", "fork-1-join");
        assertAction(decisionOrForkOrJoin, 5, "stg-2", "stg-3", "fork-1-join");
        assertAction(decisionOrForkOrJoin, 6, "stg-3", "fork-1-join", "fork-1-join");
        assertAction(decisionOrForkOrJoin, 7, "action-2", "end", "kill");
        assertThat(decisionOrForkOrJoin.get(8), instanceOf(KILL.class));
        assertEquals("kill", ((KILL) decisionOrForkOrJoin.get(8)).getName());
        assertEquals("Action failed, error message[${wf:errorMessage(wf:lastErrorNode())}]", ((KILL) decisionOrForkOrJoin.get(8)).getMessage());
    }

    private void assertForkJoin(List<Object> decisionOrForkOrJoin) {
        assertThat(decisionOrForkOrJoin.get(1), instanceOf(FORK.class));
        assertEquals("fork-1", ((FORK) decisionOrForkOrJoin.get(1)).getName());
        assertThat(((FORK) decisionOrForkOrJoin.get(1)).getPath(), hasSize(2));
        assertEquals("stg-1", ((FORK) decisionOrForkOrJoin.get(1)).getPath().get(0).getStart());
        assertEquals("stg-2", ((FORK) decisionOrForkOrJoin.get(1)).getPath().get(1).getStart());

        assertThat(decisionOrForkOrJoin.get(2), instanceOf(JOIN.class));
        assertEquals("fork-1-join", ((JOIN) decisionOrForkOrJoin.get(2)).getName());
        assertEquals("fork-1-decision", ((JOIN) decisionOrForkOrJoin.get(2)).getTo());

        assertThat(decisionOrForkOrJoin.get(3), instanceOf(DECISION.class));
        assertEquals("fork-1-decision", ((DECISION) decisionOrForkOrJoin.get(3)).getName());
        assertThat(((DECISION) decisionOrForkOrJoin.get(3)).getSwitch().getCase(), hasSize(1));
        assertEquals("kill", ((DECISION) decisionOrForkOrJoin.get(3)).getSwitch().getCase().get(0).getTo());
        assertEquals("action-2", ((DECISION) decisionOrForkOrJoin.get(3)).getSwitch().getDefault().getTo());
    }

    private void assertAction(List<Object> decisionOrForkOrJoin, int index, String name, String okTransition, String errorTransition) {
        assertThat(decisionOrForkOrJoin.get(index), instanceOf(ACTION.class));
        assertEquals(name, ((ACTION) decisionOrForkOrJoin.get(index)).getName());
        assertEquals(okTransition, ((ACTION) decisionOrForkOrJoin.get(index)).getOk().getTo());
        assertEquals(errorTransition, ((ACTION) decisionOrForkOrJoin.get(index)).getError().getTo());
    }
}