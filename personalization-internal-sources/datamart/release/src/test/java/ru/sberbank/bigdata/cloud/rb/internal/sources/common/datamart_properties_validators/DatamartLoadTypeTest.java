package ru.sberbank.bigdata.cloud.rb.internal.sources.common.datamart_properties_validators;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.FullReplace;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.HistoryUpdate;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.Increment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.PartialReplace;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.*;

import java.io.IOException;
import java.util.Objects;
import java.util.stream.Stream;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.datamart_properties_validators.ClassLoadUtils.findClasses;

class DatamartLoadTypeTest {

    @Test
    @DisplayName("Проверяет что указанный тип загрузки витрины подходит для класса")
    void testDatamartLoadIsCorrect() throws IOException, ClassNotFoundException {
        findClasses().stream()
                .filter(aClass -> ClassLoadUtils.superClasses(aClass).contains(Datamart.class))
                .forEach(DatamartLoadTypeTest::assertDatamartTypeIsCorrect);
    }

    private static void assertDatamartTypeIsCorrect(final Class<?> aClass) {
        final Increment incremental = aClass.getAnnotation(Increment.class);
        final HistoryUpdate historyUpdate = aClass.getAnnotation(HistoryUpdate.class);
        final FullReplace fullReplace = aClass.getAnnotation(FullReplace.class);
        final PartialReplace partialReplace = aClass.getAnnotation(PartialReplace.class);

        final Class<?> superclass = aClass.getSuperclass();
        final long types = Stream.of(incremental, historyUpdate, fullReplace, partialReplace)
                .filter(Objects::nonNull)
                .count();

        if (superclass.equals(StagingDatamart.class) || superclass.equals(ReplicaBasedStagingDatamart.class)) {
            if (types > 0) {
                throw new AssertionError("Loading annotation is unexpectable for Staging datamarts: " + aClass.getName());
            }
            return;
        }

        if (types > 1) {
            throw new AssertionError("Only one datamart loading type annotation expected at " + aClass.getName()
                    + " but found: " + types);
        }

        if (types == 0) {
            throw new AssertionError("Datamart loading type annotation expected but not found at: " + aClass.getName());
        }

        if (incremental != null && !superclass.equals(Datamart.class)) {
            throw new AssertionError("Datamart parent expected for Incremental loading: " + aClass.getName());
        }

        if (historyUpdate != null && !superclass.equals(HistoricalDatamart.class) && !superclass.equals(ReplicaBasedHistoricalDatamart.class)) {
            throw new AssertionError("HistoricalDatamart parent expected for HistoryUpdate loading: " + aClass.getName());
        }

        if (fullReplace != null && !superclass.equals(Datamart.class)) {
            throw new AssertionError("Datamart parent expected for FullReplace loading: " + aClass.getName());
        }

        if (partialReplace != null && !superclass.equals(Datamart.class)) {
            throw new AssertionError("Datamart parent expected for PartialReplace loading: " + aClass.getName());
        }
    }
}
