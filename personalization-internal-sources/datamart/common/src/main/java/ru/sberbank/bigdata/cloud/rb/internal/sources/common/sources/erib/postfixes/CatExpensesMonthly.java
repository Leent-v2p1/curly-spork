package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.erib.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.erib.EribInstance;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.erib.EribSourcePostfix;

import java.util.Arrays;

/**
 * required for sbol_cat_expenses monthly datamart
 */
public class CatExpensesMonthly extends EribSourcePostfix {

    public CatExpensesMonthly(EribInstance instance) {
        super(instance, "cat-expenses-monthly", "catExpensesMonthly");
    }

    public static CatExpensesMonthly[] values() {
        return Arrays.stream(EribInstance.values())
                .map(CatExpensesMonthly::new)
                .toArray(CatExpensesMonthly[]::new);
    }
}
