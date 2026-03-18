package ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.functions;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.DatamartRuntimeException;

import java.util.function.BiConsumer;

public interface SneakyThrowBiConsumer<T, U> {
    static <T, U> BiConsumer<T, U> sneakyThrow(SneakyThrowBiConsumer<T, U> mapper) {
        return (t, u) -> {
            try {
                mapper.accept(t, u);
            } catch (Exception e) {
                throw new DatamartRuntimeException(e);
            }
        };
    }

    void accept(T t, U u) throws Exception;
}
