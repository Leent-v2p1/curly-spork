package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.builders.actions;

import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.builders.ExecutionShellSparkActionSetter;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.source.workflow.ACTION;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.ActionConf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class ShellActionParamsBuilderTest {

    @Test
    void testBuildActionParams() {
        ExecutionShellSparkActionSetter actionSetter = mock(ExecutionShellSparkActionSetter.class);
        ShellActionParamsBuilder paramsBuilder = new ShellActionParamsBuilder(
                "action_1", new ActionConf(), Environment.PRODUCTION, actionSetter);
        ACTION action = paramsBuilder.buildActionParams();
        assertEquals("action-1", action.getName());
        verify(actionSetter, times(1)).setAction(any(), any());
    }
}
