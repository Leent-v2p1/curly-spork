package ru.sberbank.bigdata.cloud.rb.internal.sources.common.diagnostic.graphite;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.diagnostic.DynamicMetricsListener;

/**
 * Класс публикует количество выделенных executor-ов все время работы приложения
 */
public class ExecutorAllocationListener implements DynamicMetricsListener<Integer> {

    private static final Logger log = LoggerFactory.getLogger(ExecutorAllocationListener.class);
    private final Environment env;
    private final CurrentExecutorGauge currentExecutor = new CurrentExecutorGauge();
    private ScheduledReporter reporter;
    private MetricRegistry registry;

    public ExecutorAllocationListener(ScheduledReporter reporter, MetricRegistry registry, Environment env) {
        this.reporter = reporter;
        this.registry = registry;
        this.env = env;
    }

    @Override
    public void onStateChange(Integer state) {
        if (env.isTestEnvironment()) {
            log.info("Current environment is {}. Class will not publish metric.", env);
            return;
        }
        currentExecutor.setState(state);
        registry.gauge("allocated_executors", () -> currentExecutor);
        log.info("sending updated allocated executors state {}", state);
        reporter.report();
    }

    static class CurrentExecutorGauge implements Gauge<Integer> {

        private Integer state;

        public void setState(Integer state) {
            this.state = state;
        }

        @Override
        public Integer getValue() {
            return state;
        }
    }
}
