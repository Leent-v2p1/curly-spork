package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.history;

import org.apache.spark.storage.StorageLevel;

public class PersistJoinStrategy extends DefaultJoinStrategy {
    private final StorageLevel storageLevel;

    public PersistJoinStrategy(StorageLevel storageLevel) {
        this.storageLevel = storageLevel;
    }

    public static JoinStrategy of(StorageLevel storageLevel) {
        return new PersistJoinStrategy(storageLevel);
    }

    @Override
    public StorageLevel storageLevel() {
        return storageLevel;
    }
}
