package ru.sberbank.bigdata.cloud.rb.internal.sources.common.diagnostic.graphite;

import com.codahale.metrics.ScheduledReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.auto_config.DatamartEventsListener;

public class GraphiteListener implements DatamartEventsListener {

    private static final Logger log = LoggerFactory.getLogger(GraphiteListener.class);

    private final ScheduledReporter reporter;

    public GraphiteListener(ScheduledReporter reporter) {
        this.reporter = reporter;
    }

    @Override
    public void onDatamartBuildEnd() {
        log.info("Stopping GraphiteReporter because datamart build ended");
        reporter.stop();
    }
}
