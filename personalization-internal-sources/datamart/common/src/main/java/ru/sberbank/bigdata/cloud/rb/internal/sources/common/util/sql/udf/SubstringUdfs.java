package ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.udf;

import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF3;

public class SubstringUdfs {

    public static UDF3<String, Integer, Integer, String> substringSql() {
        return (str, pos, len) -> {
            if (str == null || pos == null || len == null) {
                return null;
            }
            final int length = str.length();
            if ((pos - 1) >= length) {
                return "";
            }
            if (len > length - (pos - 1)) {
                return str.substring(pos - 1);
            }
            return str.substring(pos - 1, (pos - 1) + len);
        };
    }

    public static UDF2<String, Integer, String> substr() {
        return (str, strIndex) -> {
            if (str != null && strIndex != null) {
                return str.length() >= strIndex - 1 ? str.substring(strIndex - 1) : "";
            }
            return null;
        };
    }
}
