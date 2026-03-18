package ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.udf;

import org.apache.spark.sql.api.java.UDF2;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

public class MonthsBetween implements UDF2<Timestamp, Timestamp, Long> {

    @Override
    public Long call(Timestamp timestamp1, Timestamp timestamp2) throws Exception {
        if (timestamp1 == null || timestamp2 == null) {
            return null;
        }
        LocalDateTime time1 = timestamp1.toLocalDateTime();
        LocalDateTime time2 = timestamp2.toLocalDateTime();
        long months = ChronoUnit.MONTHS.between(time2, time1);
        long days = ChronoUnit.DAYS.between(time2.plusMonths(months), time1);
        return days >= 15 ? months + 1 : months;
    }
}
