package ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.skew_partitions;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class SkewedPartitionAnalyzer {

    private static final Logger log = LoggerFactory.getLogger(SkewedPartitionAnalyzer.class);

    /**
     * @return list of tuples (count1, count2),
     * where count1, count2 - count of elements in partitions according to DFs and distribution
     */
    public List<Tuple2<Long, Long>> analyze(Dataset<Row> df1, String id1, Dataset<Row> df2, String id2) {

        JavaRDD<Row> df1Rdd = df1.javaRDD();
        JavaRDD<Row> df2Rdd = df2.javaRDD();

        final int numPartitionsInDf1 = df1Rdd.getNumPartitions();
        final int numPartitionsInDf2 = df2Rdd.getNumPartitions();
        log.info("df1Rdd.getNumPartitions() {}", numPartitionsInDf1);
        log.info("df2Rdd.getNumPartitions() {}", numPartitionsInDf2);

        int max = Math.max(numPartitionsInDf1, numPartitionsInDf2);

        Map<Integer, Long> df1Partitions = countPartitionElems(df1Rdd, id1, max);
        Map<Integer, Long> df2Partitions = countPartitionElems(df2Rdd, id2, max);

        Set<Integer> keys = new HashSet<>(df1Partitions.keySet());
        keys.addAll(df2Partitions.keySet());

        return keys.stream()
                .map(key -> new Tuple2<>(df1Partitions.get(key), df2Partitions.get(key)))
                .collect(toList());
    }

    /**
     * @return map, where key is hashCode and value - count of key
     */
    public Map<Integer, Long> countPartitionElems(JavaRDD<Row> rdd, String id, int numPartitions) {
        return rdd
                .mapToPair(new HashCodeMapper(numPartitions, id))
                .countByKey()
                .entrySet().stream()
                .collect(toMap(Map.Entry::getKey, entry -> ((Long) entry.getValue())));
    }
}
