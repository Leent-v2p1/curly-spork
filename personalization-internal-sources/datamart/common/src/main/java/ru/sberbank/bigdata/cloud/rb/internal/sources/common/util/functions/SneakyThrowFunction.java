package ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.functions;

import java.util.function.Function;

public interface SneakyThrowFunction<T, R> {
    static <T, R> Function<T, R> sneakyThrow(SneakyThrowFunction<T, R> mapper) {
        return t -> {
            try {
                return mapper.apply(t);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    R apply(T t) throws Exception;
}
