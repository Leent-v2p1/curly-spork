package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.builders.actions;

import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.source.workflow.ACTION;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.ActionConf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class SubWfActionParamsBuilderTest {
    @Test
    void resolveSubWf() {
        final ActionConf actionConf = new ActionConf();
        actionConf.setName("custom_rb_cod.action_1");
        actionConf.setType("sub-wf");

        SubWfActionParamsBuilder builder = new SubWfActionParamsBuilder(actionConf.getName(), actionConf, Environment.PRODUCTION);
        ACTION oozieAction = builder.buildActionParams();

        assertEquals("custom-rb-cod-action-1", oozieAction.getName());
        assertNotNull(oozieAction.getSubWorkflow());
        assertNotNull(oozieAction.getSubWorkflow().getPropagateConfiguration());
        assertEquals("/oozie-app/wf/custom/rb/production/cod/generated/create-action-1-wf", oozieAction.getSubWorkflow().getAppPath());
    }
}