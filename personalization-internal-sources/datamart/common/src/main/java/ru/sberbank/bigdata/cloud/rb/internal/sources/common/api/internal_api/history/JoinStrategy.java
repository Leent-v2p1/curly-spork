package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.history;

import org.apache.spark.sql.Column;
import org.apache.spark.storage.StorageLevel;

public interface JoinStrategy {

    Column condition(Column[] idsLeft, Column[] idsRight);

    Column isNull(Column[] columns);

    Column isNotNull(Column[] columns);

    StorageLevel storageLevel();
}
