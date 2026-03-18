package ru.sberbank.bigdata.cloud.rb.internal.sources.common.util;

public class StringUtils {

    public static Integer toInt(String intStr) {
        if (intStr != null && !intStr.isEmpty()) {
            return Integer.valueOf(intStr);
        }
        return null;
    }

    public static boolean isBlank(CharSequence cs) {
        return cs == null || cs.toString().matches("\\s*");
    }

    public static boolean isNotBlank(CharSequence cs) {
        return !isBlank(cs);
    }

    public static String removePostfix(String source, String postfix) {
        return source.replaceAll(postfix + "$", "");
    }

    public static String replaceLast(String source, String toReplace, String append) {
        int indexOf = source.lastIndexOf(toReplace);
        if (indexOf == -1) {
            return source;
        }
        String substring = source.substring(0, indexOf);
        return substring + append;
    }
}
