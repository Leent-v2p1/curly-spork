package ru.sberbank.bigdata.cloud.rb.internal.sources.common.diagnostic.graphite;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.codahale.metrics.graphite.GraphiteSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.DatamartNaming;

import java.util.concurrent.TimeUnit;

public class GraphiteListenerCreator {

    private static final Logger log = LoggerFactory.getLogger(GraphiteListenerCreator.class);

    private final DatamartNaming naming;
    private final GraphiteSender sender;
    private ScheduledReporter reporter;
    private MetricRegistry registry;

    public GraphiteListenerCreator(DatamartNaming naming, GraphiteSender sender) {
        this.naming = naming;
        this.sender = sender;
    }

    static String graphiteMetricPrefix(DatamartNaming naming) {
        final String schema = naming.resultSchema();
        final String table = naming.resultTable();
        final String graphitePrefix = String.format("dc.custom.rozn.masspers.%s.%s", schema, table);
        log.info("resolved graphitePrefix = {}", graphitePrefix);
        return graphitePrefix;
    }

    public GraphiteMetricsProvider createDatamartMetricProvider(Environment environment) {
        return new GraphiteMetricsProvider(metricsReporter(), metricsRegistry(), environment);
    }

    public ExecutorAllocationListener createExecutorsMetricProvider(Environment environment) {
        return new ExecutorAllocationListener(metricsReporter(), metricsRegistry(), environment);
    }

    public CurrentTasksListener createTaskMetricProvider(Environment environment) {
        return new CurrentTasksListener(metricsReporter(), metricsRegistry(), environment);
    }

    public ScheduledReporter metricsReporter() {
        final MetricRegistry metricRegistry = metricsRegistry();
        if (this.reporter == null) {
            if (sender != null) {
                log.warn("graphite is available(sender is not null), creating graphite reporter");
                this.reporter = GraphiteReporter.forRegistry(metricRegistry)
                        .prefixedWith(graphiteMetricPrefix(naming))
                        .convertDurationsTo(TimeUnit.MILLISECONDS)
                        .filter(MetricFilter.ALL)
                        .build(sender);
            } else {
                log.warn("graphite is not available(sender is null), creating log reporter");
                this.reporter = new LogReporter(metricRegistry,
                        "log-metrics-reporter",
                        MetricFilter.ALL,
                        TimeUnit.SECONDS,
                        TimeUnit.MILLISECONDS,
                        LoggerFactory.getLogger(LogReporter.class));
            }
            this.reporter.start(1, TimeUnit.SECONDS);
        }
        return reporter;
    }

    private MetricRegistry metricsRegistry() {
        if (this.registry == null) {
            log.info("creating graphiteMetricsRegistry");
            this.registry = new MetricRegistry();
        }
        return registry;
    }
}
