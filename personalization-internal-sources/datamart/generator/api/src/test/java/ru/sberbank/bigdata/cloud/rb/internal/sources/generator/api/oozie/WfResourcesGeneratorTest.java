package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie;

import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.properties.DatamartPropertiesGenerator;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.shell.ShellGenerator;

import java.util.Collections;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class WfResourcesGeneratorTest {

    @Test
    void testGenerateWfResources() {
        WfGenerator wfGenerator = mock(WfGenerator.class);
        ShellGenerator shellGenerator = mock(ShellGenerator.class);
        DatamartPropertiesGenerator propertiesGenerator = mock(DatamartPropertiesGenerator.class);
        SparkActionFilter actionFilter = mock(SparkActionFilter.class);
        WfResourcesGenerator wfResourcesGenerator = new WfResourcesGenerator(wfGenerator, shellGenerator,
                propertiesGenerator, actionFilter);
        wfResourcesGenerator.generate(Collections.emptyList(), "workflow-name");

        verify(actionFilter).get(anyList());
        verify(wfGenerator).generate(anyList(), anyString());
        verify(shellGenerator).generate(anyList());
        verify(propertiesGenerator).generate(anyList());
    }
}
