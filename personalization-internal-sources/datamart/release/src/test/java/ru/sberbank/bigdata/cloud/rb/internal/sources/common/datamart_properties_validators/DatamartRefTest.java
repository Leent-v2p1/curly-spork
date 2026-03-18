package ru.sberbank.bigdata.cloud.rb.internal.sources.common.datamart_properties_validators;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.PropertiesUtils;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.DatamartRef;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.Datamart;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toList;

class DatamartRefTest {

    @Test
    @DisplayName("Проверяет что id указанное в аннотации @DatamartRef есть в datamart-properties.yaml")
    void testDatamartRefIdIsCorrect() throws IOException, ClassNotFoundException {
        final List<Class<?>> classes = ClassLoadUtils.findClasses();

        final List<String> actualDatamartIds = classes.stream()
                .filter(aClass -> ClassLoadUtils.superClasses(aClass).contains(Datamart.class))
                .filter(aClass -> aClass.isAnnotationPresent(DatamartRef.class))
                .map(aClass -> aClass.getAnnotation(DatamartRef.class))
                .filter(annotation -> !annotation.useSystemPropertyToGetId())
                .map(DatamartRef::id)
                .collect(toList());

        final Set<String> allDatamartIds = PropertiesUtils.getAllActionsId();
        assertDatamartIdIsCorrect(actualDatamartIds, allDatamartIds);
    }

    private static void assertDatamartIdIsCorrect(List<String> actualDatamartIds, Set<String> allDatamartIds) {
        final List<String> notValidDatamartIds = new ArrayList<>();
        for (String actualDatamartId : actualDatamartIds) {
            final boolean isDatamartIdValid = allDatamartIds.contains(actualDatamartId);
            if (!isDatamartIdValid) {
                notValidDatamartIds.add(actualDatamartId);
            }
        }
        if (!notValidDatamartIds.isEmpty()) {
            throw new AssertionError("Can't find these datamartIds in datamart-properties.yaml: " + notValidDatamartIds);
        }
    }
}
