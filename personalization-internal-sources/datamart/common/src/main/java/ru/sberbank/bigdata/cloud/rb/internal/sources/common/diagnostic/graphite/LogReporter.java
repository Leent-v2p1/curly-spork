package ru.sberbank.bigdata.cloud.rb.internal.sources.common.diagnostic.graphite;

import com.codahale.metrics.*;
import org.slf4j.Logger;

import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

/**
 * Class to replace GraphiteReporter, if graphite is not available for current environment.
 * This implementation will send all reported metrics to logger
 */
public class LogReporter extends ScheduledReporter {

    private final Logger log;

    protected LogReporter(MetricRegistry registry,
                          String name,
                          MetricFilter filter,
                          TimeUnit rateUnit,
                          TimeUnit durationUnit,
                          Logger log) {
        super(registry, name, filter, rateUnit, durationUnit);
        this.log = log;
    }

    /**
     * @param gauges only this metric type is used in our project, so all other types would be ignored
     */
    @Override
    public void report(SortedMap<String, Gauge> gauges,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {
        gauges.forEach((keyName, gauge) -> log.debug("log metric with name {} value is {}", keyName, gauge.getValue()));
    }
}
