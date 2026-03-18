package ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.skew_partitions;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.io.Serializable;

public class TablePartitionAnalyzer implements Serializable {

    public Tuple2<JavaPairRDD<Integer, Integer>, JavaPairRDD<Object, Integer>> createReport(JavaRDD<Row> tableRdd, String id) {
        final int numPartitions = tableRdd.getNumPartitions();
        final JavaPairRDD<Integer, Integer> hashModCountPairs = countPartitionElems(tableRdd, id, numPartitions);
        JavaPairRDD<Object, Integer> idCountPairs = null;
        if (!hashModCountPairs.isEmpty()) {
            final Tuple2<Integer, Integer> largestPartition = hashModCountPairs.first();
            final Integer largestPartitionHashMod = largestPartition._1();

            idCountPairs = countElemInPartition(tableRdd, id, numPartitions, largestPartitionHashMod);
        }
        return new Tuple2<>(hashModCountPairs, idCountPairs);
    }

    private JavaPairRDD<Integer, Integer> countPartitionElems(JavaRDD<Row> tableRdd, String id, int numPartitions) {
        return tableRdd
                .mapToPair(new HashCodeMapper(numPartitions, id))
                .foldByKey(0, Integer::sum)
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                .mapToPair(Tuple2::swap);
    }

    private JavaPairRDD<Object, Integer> countElemInPartition(JavaRDD<Row> tableRdd,
                                                              String id,
                                                              int numPartitions,
                                                              Integer partitionHashMod) {
        return tableRdd
                .mapToPair(row -> {
                    final Integer hashCodeMod = new HashCodeMapper(numPartitions, id).call(row)._1();
                    return new Tuple2<>(hashCodeMod, row.getAs(id));
                })
                .filter(hashModAndId -> partitionHashMod.equals(hashModAndId._1()))
                .mapToPair(hashModAndId -> new Tuple2<>(hashModAndId._2(), 1))
                .foldByKey(0, Integer::sum)
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                .mapToPair(Tuple2::swap);
    }
}
