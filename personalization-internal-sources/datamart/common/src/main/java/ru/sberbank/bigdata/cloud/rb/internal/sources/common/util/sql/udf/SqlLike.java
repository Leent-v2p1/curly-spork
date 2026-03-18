package ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.udf;

import org.apache.spark.sql.api.java.UDF3;

import java.util.regex.Pattern;

/**
 * Created by sbt-zagrebin-ar on 24.08.2017.
 * Имитирует LIKE из SQL
 * arg1 - строка
 * arg2 - шаблон регулярного выражения
 * arg3 - флаг из java.util.regex.Pattern (если флаг не нужен, то 0)
 */
public class SqlLike implements UDF3<String, String, Integer, Boolean> {

    @Override
    public Boolean call(String str, String template, Integer flag) {
        return str == null ? null : Pattern.compile(template, flag).matcher(str).matches();
    }
}
