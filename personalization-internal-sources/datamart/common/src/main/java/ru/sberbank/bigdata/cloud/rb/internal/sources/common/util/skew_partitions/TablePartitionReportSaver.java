package ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.skew_partitions;

import org.apache.spark.api.java.JavaPairRDD;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.file.HDFSHelper;

import static java.util.stream.Collectors.joining;

public class TablePartitionReportSaver {

    private final String userHomePath;

    public TablePartitionReportSaver(String userHomePath) {
        this.userHomePath = userHomePath;
    }

    public void save(JavaPairRDD<?, ?> rdd, String tableName, String id, String fileName) {
        final String csvReport = rdd
                .collect()
                .stream()
                .map(tuple -> tuple._1() + "," + tuple._2())
                .collect(joining(System.lineSeparator()));
        HDFSHelper.rewriteFile(csvReport, String.join("/", userHomePath, tableName, id, fileName));
    }
}
