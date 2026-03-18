package ru.sberbank.bigdata.cloud.rb.internal.sources.common.datamart_properties_validators;

import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.base.WorkflowType;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.DatamartProperties;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.PropertiesUtils;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;

import java.util.Set;

import static junit.framework.Assert.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

class CheckClassNamesTest {

    @Test
    void testClassIsExist() {
        ClassLoader classLoader = this.getClass().getClassLoader();

        Set<String> datamartsIdSet = PropertiesUtils.getAllActionsIdWithType(WorkflowType.DATAMART);
        Set<String> stagesIdSet = PropertiesUtils.getAllActionsIdWithType(WorkflowType.STAGE);
        datamartsIdSet.addAll(stagesIdSet);
        datamartsIdSet.forEach(datamartId -> {
                    String className = new DatamartProperties(FullTableName.of(datamartId)).getClassName();
                    String testFailedMessage = "Failed to find class with full name = '" + className + "'," +
                            " datamart name = '" + datamartId + "'";
                    try {
                        Class<?> aClass = classLoader.loadClass(className);
                        assertNotNull(testFailedMessage, aClass);
                    } catch (ClassNotFoundException e) {
                        fail(testFailedMessage);
                    }
                }
        );
    }
}
