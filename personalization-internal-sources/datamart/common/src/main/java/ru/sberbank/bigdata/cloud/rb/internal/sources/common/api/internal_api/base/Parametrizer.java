package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.base;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.NoSuchElementException;
import java.util.Optional;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.PropertyConstants.SPARK_OPTION_PREFIX;

public class Parametrizer {
    private static final Logger log = LoggerFactory.getLogger(Parametrizer.class);
    private final SparkSession sqlContext;

    public Parametrizer(SparkSession sqlContext) {
        this.sqlContext = sqlContext;
    }

    public Dataset<Row> applyRepartitionAndCoalesce(Dataset<Row> dataframe, String tableName) {
        dataframe = repartitionIfEnabled(dataframe, tableName);
        dataframe = coalesceIfEnabled(dataframe, tableName);
        return dataframe;
    }

    public Dataset<Row> coalesceIfEnabled(final Dataset<Row> dataframe, final String tableName) {
        String paramName = tableName.isEmpty() ? "coalesce" : "coalesce." + tableName;
        Optional<String> coalescing = getDatamartParam(paramName);
        if (coalescing.isPresent()) {
            try {
                int coalesce = Integer.parseInt(coalescing.get());
                log.info("Coalescing dataframe {} by {}", tableName, coalesce);
                return dataframe.coalesce(coalesce);
            } catch (NumberFormatException ex) {
                log.error("Incorrect coalesce value {} for {}", coalescing.get(), tableName);
            }
        }
        return dataframe;
    }

    public Dataset<Row> repartitionIfEnabled(final Dataset<Row> dataframe, final String tableName) {
        String paramName = tableName.isEmpty() ? "repartition" : "repartition." + tableName;
        Optional<String> repartitioning = getDatamartParam(paramName);
        if (repartitioning.isPresent()) {
            try {
                int repartition = Integer.parseInt(repartitioning.get());
                log.info("Repartitioning dataframe {} by {}", tableName, repartition);
                return dataframe.repartition(repartition);
            } catch (NumberFormatException ex) {
                log.error("Incorrect repartition value {} for {}", repartitioning.get(), tableName);
            }
        }
        return dataframe;
    }

    public boolean checkCondition(final String param, final boolean defaultValue) {
        Optional<String> conditional = getDatamartParam(param);
        if (conditional.isPresent()) {
            boolean condition = Boolean.parseBoolean(conditional.get());
            log.info("Custom logic of {} is {}", param, condition);
            return condition;
        }
        return defaultValue;
    }

    public Optional<String> getDatamartParam(final String param) {
        try {
            return Optional.of(sqlContext.sparkContext().conf().get(SPARK_OPTION_PREFIX + param));
        } catch (NoSuchElementException ex) {
            return Optional.empty();
        }
    }
}
