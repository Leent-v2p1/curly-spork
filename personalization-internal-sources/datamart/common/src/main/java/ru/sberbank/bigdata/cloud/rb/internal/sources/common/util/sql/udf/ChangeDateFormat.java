package ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.udf;

import org.apache.spark.sql.api.java.UDF1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * Преобразование строки в дату.
 * Формат строки: YYMM
 * Формат даты: YYYY-MM-dd 23:59:59, где dd - последний день месяца MM
 *
 * @return YYYY-MM-dd 23:59:59
 */
public class ChangeDateFormat implements UDF1<String, Timestamp> {

    private static final Logger log = LoggerFactory.getLogger(ChangeDateFormat.class);

    @Override
    public Timestamp call(String date) {
        if (date == null || date.length() != 4) {
            log.info("Wrong input date format for string - {}. It should be 'YYMM'!", date);
            return null;
        }
        String stringYearMonth = 20 + date.substring(0, 2) + "-" + date.substring(2);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM");
        Timestamp resultDate;
        try {
            YearMonth yearMonth = YearMonth.parse(stringYearMonth, formatter);
            int daysToMonth = yearMonth.lengthOfMonth();
            resultDate = Timestamp.valueOf(stringYearMonth + "-" + daysToMonth + " 23:59:59");
        } catch (DateTimeParseException e) {
            log.info("Wrong input date format for string - {}. It should be 'YYMM'!", date);
            return null;
        }
        return resultDate;
    }
}
