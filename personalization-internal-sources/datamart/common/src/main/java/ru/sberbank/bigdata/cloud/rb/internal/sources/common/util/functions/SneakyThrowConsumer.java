package ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.functions;

import java.util.function.Consumer;

public interface SneakyThrowConsumer<T> {
    static <T> Consumer<T> sneakyThrow(SneakyThrowConsumer<T> mapper) {
        return t -> {
            try {
                mapper.accept(t);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    void accept(T t) throws Exception;
}
