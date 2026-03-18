package ru.sberbank.bigdata.cloud.rb.internal.sources.reporter;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

class SourceNameParserTest {

    private final List<String> wfNames = asList(
            "masspers-clsklrozn-test-mdm-datamarts",
            "masspers-clsklrozn-erib-ikfl4-oper-ahd-datamarts",
            "infa.ap_cg_gMainHDP.masspers-clsklrozn.internal_erib_operations_ikfl_4",
            "masspers-clsklrozn-erib-ikfl5-datamarts",
            "masspers-clsklrozn-cod-daily-datamarts",
            "masspers-clsklrozn-cod-55-monthly-datamarts",
            "masspers-clsklrozn-risk-triggers-daily-datamarts",
            "masspers-clsklrozn-nba-counts-streaming",
            "dummy_workflow",
            "masspers-clsklrozn-way4-currency-daily-datamarts",
            "masspers-clsklrozn-ekp-oper-daily-datamarts"
    );

    private final String[] expected = {
            "test",
            "erib-ahd",
            "Erib2AHD",
            "erib-ikfl5",
            "cod",
            "cod-55",
            "triggers",
            "nba",
            "undefined",
            "way4",
            "ekp"
    };

    @Test
    void resolveSystemNameTest() {
        SourceNameParser nameParser = new SourceNameParser();
        List<String> systemNames = wfNames.stream()
                .map(nameParser::resolveWfSystem)
                .collect(Collectors.toList());

        assertThat(systemNames, containsInAnyOrder(expected));
    }
}