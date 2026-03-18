package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.properties;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.base.WorkflowType;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.DatamartProperties;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.NameAdditions;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.ActionConf;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans.ActionImpl;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.properties.builder.*;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;

class PropertiesBuilderMapperImplTest {

    private final String ACTION_NAME = "custom.action1";
    private final String REPARTITIONER_ACTION_NAME = "custom.action1" + NameAdditions.REPARTITIONER_POSTFIX;
    private static final String CTL_STATS_PUBLISHER_ACTION_NAME = "custom.action1" + NameAdditions.CTL_STATS_PUBLISHER_POSTFIX;

    @Test
    void testGetDatamartBuilder() {
        SparkPropertiesBuilder<? extends ActionConf> propertiesBuilder = getPropertiesBuilder(
                ACTION_NAME, WorkflowType.DATAMART);
        assertTrue(propertiesBuilder instanceof SparkPropertiesBuilder);
    }

    @Test
    void testGetStageBuilder() {
        SparkPropertiesBuilder<? extends ActionConf> propertiesBuilder = getPropertiesBuilder(ACTION_NAME,
                WorkflowType.STAGE);
        assertTrue(propertiesBuilder instanceof SparkStagePropertiesBuilder);
    }

    @Test
    void testGetReservingBuilder() {
        SparkPropertiesBuilder<? extends ActionConf> propertiesBuilder = getPropertiesBuilder(ACTION_NAME,
                WorkflowType.RESERVING);
        assertTrue(propertiesBuilder instanceof SparkReservingPropertiesBuilder);
    }

    @Test
    void testGetRepartitionerBuilder() {
        SparkPropertiesBuilder<? extends ActionConf> propertiesBuilder = getPropertiesBuilder(REPARTITIONER_ACTION_NAME,
                WorkflowType.REPARTITIONER);
        assertTrue(propertiesBuilder instanceof SparkRepartitionerPropertiesBuilder);
    }

    @Test
    void testGetCtlStatsPublisherBuilder() {
        SparkPropertiesBuilder<? extends ActionConf> propertiesBuilder = getPropertiesBuilder(CTL_STATS_PUBLISHER_ACTION_NAME,
                WorkflowType.CTL_STATS_PUBLISHER);
        assertTrue(propertiesBuilder instanceof CtlStatisticPublisherPropertiesBuilder);
    }

    @Test
    void testGetHelperBuilder() {
        SparkPropertiesBuilder<? extends ActionConf> propertiesBuilder = getPropertiesBuilder(ACTION_NAME,
                WorkflowType.HELPER);
        assertTrue(propertiesBuilder instanceof SparkHelperPropertiesBuilder);
    }

    @Test
    void testGenUnknownBuilder() {
        assertThrows(UnsupportedOperationException.class, () -> getPropertiesBuilder(ACTION_NAME, WorkflowType.HISTORY_STAGE));
    }

    private SparkPropertiesBuilder<? extends ActionConf> getPropertiesBuilder(String actionName, WorkflowType stage) {
        Map<String, ActionConf> properties = getProperties(actionName, stage);
        Map<String, Object> mockProperties = new HashMap<>();
        mockProperties.put(actionName, null);
        PropertiesBuilderMapperImpl mapperSpy = Mockito.spy(new PropertiesBuilderMapperImpl(properties, Environment.PRODUCTION));
        Mockito.doReturn(new DatamartProperties(FullTableName.of(actionName), mockProperties)).when(mapperSpy).getProperties(anyString());
        return mapperSpy.getPropertiesBuilder(new ActionImpl(actionName));
    }

    private Map<String, ActionConf> getProperties(String actionName, WorkflowType workflowType) {
        Map<String, ActionConf> result = new HashMap<>();
        ActionConf conf = new ActionConf();
        conf.setName(actionName);
        conf.setType(workflowType.getKey());
        result.put(actionName, conf);
        return result;
    }
}
