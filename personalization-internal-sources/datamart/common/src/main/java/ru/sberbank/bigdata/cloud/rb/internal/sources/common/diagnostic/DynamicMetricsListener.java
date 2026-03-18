package ru.sberbank.bigdata.cloud.rb.internal.sources.common.diagnostic;

public interface DynamicMetricsListener<T> {
    void onStateChange(T state);
}
