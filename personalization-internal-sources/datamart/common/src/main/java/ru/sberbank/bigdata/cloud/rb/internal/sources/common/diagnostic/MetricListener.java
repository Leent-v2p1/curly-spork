package ru.sberbank.bigdata.cloud.rb.internal.sources.common.diagnostic;

public interface MetricListener {

    void onAggregatedMetric(AggregatedMetric metric);
}
