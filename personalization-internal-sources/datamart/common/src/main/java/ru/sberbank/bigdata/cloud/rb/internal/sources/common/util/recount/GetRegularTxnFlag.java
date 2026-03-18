package ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.recount;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;

import static java.time.temporal.ChronoUnit.DAYS;

public class GetRegularTxnFlag implements Function<Row, Row> {
    public static final List<int[]> REG_TXN_INTERVAL = new ArrayList<>(
            Arrays.asList(new int[]{6, 1, 2},
                    new int[]{30, 5, 10},
                    new int[]{105, 25, 35},
                    new int[]{315, 80, 105})
    );

    private boolean checkIfRegularTxn(TreeSet<String> txnDates, String minDate, int leftBound, int rightBound) {
        int[] daysFromCurrentAgo = txnDates
                .tailSet(minDate)
                .stream()
                .mapToInt(date -> (int) DAYS.between(LocalDate.parse(date), LocalDate.now()))
                .toArray();
        Arrays.sort(daysFromCurrentAgo);

        boolean regularTxnFlag = false;

        if (daysFromCurrentAgo.length != 0) {

            int n = daysFromCurrentAgo[daysFromCurrentAgo.length - 1];

            int[] maxDataSubstring = new int[n + 1];
            for (int day : daysFromCurrentAgo) {
                maxDataSubstring[day] = 1;
                for (int i = leftBound; i <= rightBound; i++) {
                    if (day - i >= 0 && maxDataSubstring[day - i] + 1 > maxDataSubstring[day]) {
                        maxDataSubstring[day] = maxDataSubstring[day - i] + 1;
                    }
                }
                if (maxDataSubstring[day] >= 4) {
                    regularTxnFlag = true;
                    break;
                }
            }
        }

        return regularTxnFlag;
    }

    public Row call(Row r) {
        long epk = r.getLong(0);
        TreeSet<String> txn = new TreeSet<>(r.getList(1));
        int regularFlag = 0;
        for (int[] intervalData : REG_TXN_INTERVAL) {
            if (checkIfRegularTxn(txn, String.valueOf(intervalData[0]), intervalData[1], intervalData[2])) {
                regularFlag = 1;
                break;
            }
        }
        return RowFactory.create(epk, regularFlag);
    }
}