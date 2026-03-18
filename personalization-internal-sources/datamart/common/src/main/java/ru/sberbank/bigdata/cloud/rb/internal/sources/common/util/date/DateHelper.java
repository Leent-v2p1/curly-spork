package ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.date;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.DataFrameUtils;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.time.temporal.TemporalAdjusters.firstDayOfMonth;
import static java.time.temporal.TemporalAdjusters.lastDayOfMonth;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.TimestampType;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.FieldConstants.MONTH_PART_FORMAT;

/**
 * Created by sbt-buylin-ma on 13.02.2017.
 */
public class DateHelper {

    public static final DateTimeFormatter MONTH_FORMAT = new DateTimeFormatterBuilder()
            .appendValue(ChronoField.YEAR, 4)
            .appendLiteral("-")
            .appendValue(ChronoField.MONTH_OF_YEAR, 2)
            .toFormatter();

    public static final DateTimeFormatter ISO_DATE_TIME_WITHOUT_SECONDS = new DateTimeFormatterBuilder().parseCaseInsensitive()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendLiteral('T')
            .appendValue(ChronoField.HOUR_OF_DAY, 2)
            .appendLiteral(':')
            .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
            .appendLiteral('Z')
            .toFormatter();

    public static final DateTimeFormatter ISO_DATE_TIME = new DateTimeFormatterBuilder().parseCaseInsensitive()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendLiteral('T')
            .appendValue(ChronoField.HOUR_OF_DAY, 2)
            .appendLiteral(':')
            .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
            .appendLiteral(':')
            .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
            .appendLiteral('.')
            .appendValue(ChronoField.MILLI_OF_SECOND, 3)
            .appendLiteral('Z')
            .toFormatter();

    public static final DateTimeFormatter DATE_TIME = new DateTimeFormatterBuilder().parseCaseInsensitive()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .appendValue(ChronoField.HOUR_OF_DAY, 2)
            .appendLiteral(':')
            .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
            .appendLiteral(':')
            .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
            .toFormatter();

    public static final DateTimeFormatter DATE_TIME_WITH_MS = new DateTimeFormatterBuilder().parseCaseInsensitive()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .appendValue(ChronoField.HOUR_OF_DAY, 2)
            .appendLiteral(':')
            .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
            .appendLiteral(':')
            .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
            .appendLiteral('.')
            .appendValue(ChronoField.MILLI_OF_SECOND, 1)
            .toFormatter();

    private static final StructType DAY_STRUCT = new StructType()
            .add("day", TimestampType);

    private static final StructType MONTH_STRUCT = new StructType()
            .add("month", StringType);

    public static Timestamp getLastDayOfMonth(String stringDate) {
        LocalDate lastDayOfMonth = LocalDate.parse(stringDate).with(lastDayOfMonth());
        return Timestamp.valueOf(lastDayOfMonth.atStartOfDay());
    }

    public static Timestamp getFirstDayOfMonth(LocalDate date) {
        final LocalDate firstDayOfMonth = date.with(firstDayOfMonth());
        return Timestamp.valueOf(firstDayOfMonth.atStartOfDay());
    }

    public static Timestamp getFirstDayOfMonth(String stringDate) {
        final LocalDate localDate = LocalDate.parse(stringDate);
        return getFirstDayOfMonth(localDate);
    }

    public static Timestamp getFirstDayOfNextMonth(LocalDate date) {
        final LocalDate nextMonth = date.plusMonths(1L);
        return getFirstDayOfMonth(nextMonth);
    }

    public static Timestamp getFirstDayOfPreviousMonth(String stringDate) {
        LocalDate date = LocalDate
                .parse(stringDate)
                .minusMonths(1)
                .with(firstDayOfMonth());
        return Timestamp.valueOf(date.atStartOfDay());
    }

    public static Timestamp getLastDayOfPreviousMonth(String stringDate) {
        LocalDate date = LocalDate
                .parse(stringDate)
                .with(firstDayOfMonth())
                .minusDays(1);
        return Timestamp.valueOf(date.atStartOfDay());
    }

    public static Timestamp firstDayOfPreviousMonth(LocalDate date) {
        return Timestamp.valueOf(date.minusMonths(1).with(firstDayOfMonth()).atStartOfDay());
    }

    public static Timestamp firstDayOfCurrentMonth(LocalDate date) {
        return Timestamp.valueOf(date.with(firstDayOfMonth()).atStartOfDay());
    }

    public static Timestamp lastDayOfPreviousMonth(LocalDate date) {
        return Timestamp.valueOf(date.minusMonths(1).with(lastDayOfMonth()).atStartOfDay());
    }

    public static Timestamp dateMinusMonths(LocalDate date, int monthsToSubtract) {
        return Timestamp.valueOf(date.minusMonths(monthsToSubtract).with(firstDayOfMonth()).atStartOfDay());
    }

    public static String getPreviousMonth(LocalDate date) {
        return date.minusMonths(1).format(MONTH_FORMAT);
    }

    public static Timestamp startOfDay(LocalDateTime dateTime) {
        return Timestamp.valueOf(dateTime.toLocalDate().atStartOfDay());
    }

    public static Timestamp startOfYesterday(LocalDateTime dateTime) {
        return Timestamp.valueOf(dateTime.minusDays(1).toLocalDate().atStartOfDay());
    }

    public static Timestamp startOfDay(LocalDate date) {
        return Timestamp.valueOf(date.atStartOfDay());
    }

    public static Timestamp startOfYesterday(LocalDate date) {
        return Timestamp.valueOf(date.minusDays(1).atStartOfDay());
    }

    public static Timestamp endOfYesterday(LocalDate date) {
        return Timestamp.valueOf(date.minusDays(1).atTime(LocalTime.MAX));
    }

    public static Timestamp nextDay(Timestamp date) {
        return Timestamp.valueOf(date.toLocalDateTime().plusDays(1));
    }

    /**
     * Generate list of days in types described by data mapper
     * Dates difference calculated with time,
     * so last day will be included in list only
     * when time difference more than 24 hours
     *
     * @param fromDateTime date with time
     * @param toDateTime date with time
     * @param mapper function that converts timestamp to necessary datatype
     *
     * @return List of days
     *
     * @throws IllegalArgumentException when fromDay > toDay
     */
    public static <R> List<R> generateDays(final LocalDateTime fromDateTime,
                                           final LocalDateTime toDateTime,
                                           Function<LocalDateTime, ? extends R> mapper) {
        long days = ChronoUnit.DAYS.between(fromDateTime, toDateTime);
        if (days < 0) {
            throw new IllegalArgumentException("fromDay can't be more than toDay");
        }
        return Stream.iterate(fromDateTime, date -> date.plusDays(1))
                .limit(days)
                .map(mapper)
                .collect(Collectors.toList());
    }

    public static Dataset<Row> generateDays(final SparkSession sqlContext, final LocalDateTime fromDay, final LocalDateTime toDay) {
        return DataFrameUtils.createOneFieldDf(
                sqlContext,
                DAY_STRUCT,
                generateDays(fromDay, toDay, Timestamp::valueOf));
    }

    /**
     * Generate list of months in types described by data mapper
     * List includes all months starting from 'fromDate' to 'toDate' including
     *
     * If year and month of 'fromDate' is equal to year and month of 'toDate'
     * then the result list will contain only one element;
     *
     * Result list can not be empty.
     * It contains either at least 1 element either exception is thrown.
     *
     * @param fromDate from date
     * @param toDate to date
     * @param mapper function that converts date to necessary datatype
     *
     * @return List of months
     *
     * @throws IllegalArgumentException when year and month of fromDate > year and month of toDate
     */
    public static <R> List<R> generateMonths(final LocalDate fromDate,
                                             LocalDate toDate,
                                             Function<LocalDate, ? extends R> mapper) {

        long months = ChronoUnit.MONTHS.between(YearMonth.from(fromDate), YearMonth.from(toDate)) + 1;
        if (months < 0) {
            throw new IllegalArgumentException("fromDate can't be more than toDate");
        }
        return Stream.iterate(fromDate, date -> date.plusMonths(1))
                .limit(months)
                .map(mapper)
                .collect(Collectors.toList());
    }

    public static Dataset<Row> generateMonthsDataset(final SparkSession sqlContext, final LocalDate fromDate, final LocalDate toDate) {
        return DataFrameUtils.createOneFieldDf(
                sqlContext,
                MONTH_STRUCT,
                generateMonths(fromDate, toDate, date -> date.format(DateTimeFormatter.ofPattern(MONTH_PART_FORMAT))));
    }
}
