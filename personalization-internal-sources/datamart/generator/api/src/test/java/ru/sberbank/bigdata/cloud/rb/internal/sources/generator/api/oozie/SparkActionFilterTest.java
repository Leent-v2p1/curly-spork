package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie;

import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.base.WorkflowType;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.ActionConf;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans.Action;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans.ActionImpl;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans.Branch;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans.Fork;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SparkActionFilterTest {

    @Test
    void testGetAllLeaf() {
        SparkActionFilter actionFilter = new SparkActionFilter(new HashMap<>());
        List<Action> actionList = actionFilter.getAllLeaf(singletonList(createForkWithAction()));
        checkResult(actionList);
    }

    @Test
    void testFilterLeaf() {
        SparkActionFilter actionFilter = new SparkActionFilter(new HashMap<>());
        List<Action> actionList = actionFilter.getAllLeaf(singletonList(createForkWithAction()));
        checkResult(actionList);
    }

    @Test
    void testGetWfType() {
        Map<String, ActionConf> properties = getActionProperties();

        SparkActionFilter actionFilter = new SparkActionFilter(properties);
        WorkflowType wfType = actionFilter.getWfType("action1");
        assertEquals(WorkflowType.DATAMART, wfType);
    }

    @Test
    void testFilter() {
        SparkActionFilter filter = new SparkActionFilter(getActionProperties());
        List<Action> actionList = filter.get(singletonList(createForkWithAction()));
        assertEquals(1, actionList.size());
        assertEquals("action1", actionList.get(0).name());
    }

    private Map<String, ActionConf> getActionProperties() {
        Map<String, ActionConf> properties = new HashMap<>();
        ActionConf conf1 = new ActionConf();
        conf1.setName("action1");
        conf1.setType(WorkflowType.DATAMART.getKey());
        properties.put("action1", conf1);
        ActionConf conf2 = new ActionConf();
        conf2.setName("action2");
        conf2.setType(WorkflowType.SUB_WORKFLOW.getKey());
        properties.put("action2", conf2);
        return properties;
    }

    private Action createForkWithAction() {
        ActionImpl action1 = new ActionImpl("action1");
        ActionImpl action2 = new ActionImpl("action2");
        Branch branch1 = new Branch(singletonList(action1));
        Branch branch2 = new Branch(singletonList(action2));
        return new Fork("fork", Arrays.asList(branch1, branch2));
    }

    private void checkResult(List<Action> actions) {
        assertEquals(2, actions.size());
        assertEquals("action1", actions.get(0).name());
        assertEquals("action2", actions.get(1).name());
    }
}


