package ru.sberbank.bigdata.cloud.rb.internal.sources.common.diagnostic;

import java.util.ArrayList;
import java.util.List;

public class MetricProviderObserver {

    private final List<MetricListener> providerList;

    public MetricProviderObserver() {
        this.providerList = new ArrayList<>();
    }

    public void addProvider(MetricListener provider) {
        providerList.add(provider);
    }

    public void publishAggregatedMetric(AggregatedMetric metric) {
        providerList.forEach(provider -> provider.onAggregatedMetric(metric));
    }
}
