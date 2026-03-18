package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.auto_config;

import com.google.common.collect.Sets;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.*;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_strategy.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class StrategyValidator {
    private static Map<Class<? extends Datamart>, Set<Class<? extends HiveSavingStrategy>>> supportedStrategies = new HashMap<>();

    static {
        supportedStrategies.put(Datamart.class, Sets.newHashSet(ReservingSavingStrategy.class, PartitionedSavingStrategy.class));
        supportedStrategies.put(HistoricalDatamart.class, Sets.newHashSet(SeparateHistoryTableSavingStrategy.class, FirstLoadHistPartFilling.class));
        supportedStrategies.put(ReplicaBasedHistoricalDatamart.class, Sets.newHashSet(SeparateHistoryTableSavingStrategy.class, FirstLoadHistPartFilling.class));
        supportedStrategies.put(StagingDatamart.class, Sets.newHashSet(OverwriteSavingStartegy.class, CsvSavingStrategy.class));
        supportedStrategies.put(ReplicaBasedStagingDatamart.class, Sets.newHashSet(OverwriteSavingStartegy.class, CsvSavingStrategy.class));
    }

    public static boolean isSavingStrategySupported(Class<? extends Datamart> dm, Class<? extends HiveSavingStrategy> ss) {
        final Set<Class<? extends HiveSavingStrategy>> classes = supportedStrategies.get(dm);
        if (classes == null) {
            throw new IllegalStateException("Unsupported parent class " + dm);
        }
        return classes.contains(ss);
    }
}
