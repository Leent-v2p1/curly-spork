package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.replica_on_date.configuration;

import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.DatamartProperties;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.YamlPropertiesParser;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class HistoricalDatamartsExtractorTest {

    @Test
    void getHistoricalDatamarts() {
        HistoricalDatamartsExtractor historicalDatamartsExtractor = new HistoricalDatamartsExtractor();
        Map<String, Object> rawTestConfig = new YamlPropertiesParser().parse("/dm_replica_mapper/group-hist-dm.yaml");
        historicalDatamartsExtractor.setCustomProperties(rawTestConfig);
        Map<FullTableName, DatamartProperties> histDatamarts = historicalDatamartsExtractor.getHistoricalDatamarts();
        assertThat(histDatamarts.entrySet(), hasSize(3));

        DatamartProperties dm1 = histDatamarts.get(FullTableName.of("custom_rb_test.test1"));
        assertNotNull(dm1);
        assertThat(dm1.getSourceReplicaTables(), containsInAnyOrder("ac", "bd"));

        DatamartProperties dm2 = histDatamarts.get(FullTableName.of("custom_rb_autopay.ap_txn"));
        assertNotNull(dm2);
        assertThat(dm2.getSourceReplicaTables(), containsInAnyOrder("a", "b"));

        DatamartProperties dm3 = histDatamarts.get(FullTableName.of("custom_rb_autopay.payee"));
        assertNotNull(dm3);
        assertThat(dm3.getSourceReplicaTables(), containsInAnyOrder("b", "c"));
    }
}