package ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.recount;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;

import java.time.LocalDateTime;
import java.util.Optional;

import static org.apache.spark.sql.functions.broadcast;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.types.DataTypes.DateType;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.date.DateHelper.generateDays;

public class CurrencyHelper {

    /**
     * Метод заполняет пробелы в датах.
     *
     * Пропуски в 'dateColumn' заполняются в разрезе каждой 'idColumn', начиная от 'fromDate' до 'toDate' включительно.
     * Стартовое значение 'dateColumn' для каждой 'idColumn' - самая старая запись 'dateColumn' для этой колонки.
     * В результате должны быть получены значения на каждый день в разрезе 'idColumn' за период с первой даты для 'dateColumn' до 'toDate'.
     *
     * @param recount данные, в которых нужно заполнить пропуски
     * @param fromDate дата, начиная с которой нужно заполнить пропуски
     * @param toDate дата, до которой включительно нужно заполнить пропуски
     * @param idColumn Колонка, в разрезе которой будут заполняться пропуски
     * @param dateColumn Колонка, содержащая дату
     * @param sqlContext контекст для создания датасета с датами
     * @param customRepartitionNumber количество партиций для shuffle перед оконной функцией
     * @return переданный датасет 'recount' с заполненными пропусками
     */
    public static Dataset<Row> fillGaps(final Dataset<Row> recount,
                                        final LocalDateTime fromDate,
                                        final LocalDateTime toDate,
                                        final String idColumn,
                                        final String dateColumn,
                                        final SparkSession sqlContext,
                                        final Optional<Integer> customRepartitionNumber) {
        final Dataset<Row> days = generateDays(sqlContext, fromDate, toDate.plusDays(1));
        Dataset<Row> recountWithDays = recount
                .join(broadcast(days), days.col("day").cast(DateType).geq(recount.col(dateColumn).cast(DateType)));

        if (customRepartitionNumber.isPresent()) {
            recountWithDays = recountWithDays.repartition(customRepartitionNumber.get(), col(idColumn), col("day"));
        }

        return recountWithDays
                .select(recountWithDays.col("*"),
                        row_number().over(Window.partitionBy(col(idColumn), col("day")).orderBy(col(dateColumn).desc())).as("row_num"))
                .where(col("row_num").equalTo(1))
                .drop(dateColumn, "row_num")
                .withColumnRenamed("day", dateColumn);
    }

    public static Dataset<Row> fillGaps(final Dataset<Row> recount,
                                        final LocalDateTime fromDate,
                                        final LocalDateTime toDate,
                                        final String idColumn,
                                        final String dateColumn,
                                        final SparkSession sqlContext) {
        return fillGaps(recount, fromDate, toDate, idColumn, dateColumn, sqlContext, Optional.empty());
    }
}
