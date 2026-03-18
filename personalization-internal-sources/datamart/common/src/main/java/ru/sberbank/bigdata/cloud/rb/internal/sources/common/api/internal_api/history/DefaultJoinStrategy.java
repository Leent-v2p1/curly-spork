package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.history;

import org.apache.spark.sql.Column;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.Arrays;
import java.util.stream.IntStream;

public class DefaultJoinStrategy implements JoinStrategy {

    @Override
    public Column condition(Column[] idsLeft, Column[] idsRight) {
        return IntStream.range(0, idsLeft.length)
                .mapToObj(i -> new Tuple2<>(idsLeft[i], idsRight[i]))
                .map(tuple -> tuple._1().equalTo(tuple._2()))
                .reduce((condition, acc) -> acc.and(condition))
                .orElseThrow(IllegalArgumentException::new);
    }

    @Override
    public Column isNull(Column[] columns) {
        return Arrays.stream(columns)
                .map(Column::isNull)
                .reduce((col, acc) -> acc.and(col))
                .orElseThrow(IllegalArgumentException::new);
    }

    @Override
    public Column isNotNull(Column[] columns) {
        return Arrays.stream(columns)
                .map(Column::isNotNull)
                .reduce((col, acc) -> acc.and(col))
                .orElseThrow(IllegalArgumentException::new);
    }

    @Override
    public StorageLevel storageLevel() {
        return StorageLevel.NONE();
    }
}
