package ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.Month;
import java.time.format.DateTimeFormatter;

public class DateConstants {
    public static final LocalDate FIRST_CORRECT_DATE = LocalDate.of(1900, Month.JANUARY, 1);
    public static final LocalDate FIRST_LOCAL_DATE_FOR_AGGREGATION = LocalDate.of(2013, Month.JANUARY, 1);
    public static final Timestamp FIRST_DATE_FOR_AGGREGATION = Timestamp.valueOf(FIRST_LOCAL_DATE_FOR_AGGREGATION.atStartOfDay());
    public static final Timestamp MAX_DATETIME = Timestamp.valueOf("9999-12-31 00:00:00.0");
    public static final DateTimeFormatter EFFECTIVE_TIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
}
