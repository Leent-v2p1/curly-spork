package ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.skew_partitions;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.io.Serializable;

public class HashCodeMapper implements PairFunction<Row, Integer, Integer>, Serializable {

    private final int numPartitions;
    private String id;

    public HashCodeMapper(int numPartitions, String id) {
        this.numPartitions = numPartitions;
        this.id = id;
    }

    public static int nonNegativeMod(int x, int mod) {
        int rawMod = x % mod;
        return rawMod + ((rawMod < 0) ? mod : 0);
    }

    @Override
    public Tuple2<Integer, Integer> call(Row row) throws Exception {
        Object idObj = row.getAs(id);
        return new Tuple2<>(nonNegativeMod(idObj != null ? idObj.hashCode() : 0, numPartitions), 1);
    }
}
