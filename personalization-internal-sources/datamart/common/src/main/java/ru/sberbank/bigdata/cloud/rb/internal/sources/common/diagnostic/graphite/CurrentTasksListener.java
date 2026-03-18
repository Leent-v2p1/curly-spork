package ru.sberbank.bigdata.cloud.rb.internal.sources.common.diagnostic.graphite;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.diagnostic.DynamicMetricsListener;

/**
 * Класс публикует количество работающих задач при запуске новой\завершении работающей
 */
public class CurrentTasksListener implements DynamicMetricsListener<Integer> {

    private static final Logger log = LoggerFactory.getLogger(CurrentTasksListener.class);

    private final ScheduledReporter reporter;
    private final MetricRegistry registry;
    private final Environment env;
    private final CurrentTasksGauge runningTasks = new CurrentTasksGauge();

    public CurrentTasksListener(ScheduledReporter reporter, MetricRegistry registry, Environment env) {
        this.reporter = reporter;
        this.registry = registry;
        this.env = env;
    }

    @Override
    public void onStateChange(Integer state) {
        if (env.isTestEnvironment()) {
            log.info("Current environment is {}. Class will not publish metric", env);
            return;
        }
        runningTasks.setState(state);
        registry.gauge("running_tasks", () -> runningTasks);
        log.debug("sending updated tasks number {}", state);
        reporter.report();
    }

    static class CurrentTasksGauge implements Gauge<Integer> {

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
