package ru.sberbank.bigdata.cloud.rb.internal.sources.common.util;

public class Checker {

    public static void checkCondition(boolean condition, String message) {
        if (condition) {
            throw new IllegalArgumentException(message);
        }
    }

    public static void checkCondition(boolean condition, String message, Object... args) {
        if (condition) {
            throw new IllegalArgumentException(String.format(message, args));
        }
    }
}
