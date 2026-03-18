package ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.skew_partitions;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Tuple2;

/**
 * Wrapper for join that adds partition analyze logs
 */
public class JoinAnalyzer {
    private final TablePartitionReportSaver saver;
    private Integer counter = 0;
    private TablePartitionAnalyzer tablePartitionAnalyzer;

    public JoinAnalyzer(TablePartitionAnalyzer tablePartitionAnalyzer, TablePartitionReportSaver saver) {
        this.tablePartitionAnalyzer = tablePartitionAnalyzer;
        this.saver = saver;
    }

    public Dataset<Row> join(String joinId, Dataset<Row> df1, Dataset<Row> df2, Column col1, Column col2, String joinType) {
        logReport(df1, col1, joinId);
        logReport(df2, col2, joinId);

        return df1.join(df2, col1.equalTo(col2), joinType);
    }

    private void logReport(Dataset<Row> df, Column col, String joinId) {
        final String colName = col.expr().simpleString(1024).replaceAll("#.*$", "");
        final String tempColName = "idCol_" + counter++;
        final Dataset<Row> testDF = df.withColumn(tempColName, col);

        final Tuple2<JavaPairRDD<Integer, Integer>, JavaPairRDD<Object, Integer>> report = tablePartitionAnalyzer.createReport(testDF.javaRDD(), tempColName);

        saver.save(report._1(), joinId, colName, "hashmod_count_pairs.csv");
        if (report._2() != null) {
            saver.save(report._2(), joinId, colName, "id_count_pairs.csv");
        }
    }
}
