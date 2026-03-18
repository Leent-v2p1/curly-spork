package ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.entitities;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class CtlLoadingInfoTest {

    @Test
    @DisplayName("если wf_system разные, сравнивается по wf_system")
    void differentSystems() {
        CtlLoadingInfo ctlLoadingInfoFirst = result("A-SYSTEM", null, null);
        CtlLoadingInfo ctlLoadingInfoSecond = result("B-SYSTEM", null, null);
        assertTrue(ctlLoadingInfoFirst.compareTo(ctlLoadingInfoSecond) < 0);
    }

    @Test
    @DisplayName("если wf_system одинаковые и оба start_running == null, то сравнивается по start_loading")
    void sameSystemNoLoading() {
        CtlLoadingInfo ctlLoadingInfoFirst = result("A-SYSTEM", "2010-01-01 10:01:00.0", null);
        CtlLoadingInfo ctlLoadingInfoSecond = result("A-SYSTEM", "2010-01-01 10:02:00.0", null);
        assertTrue(ctlLoadingInfoFirst.compareTo(ctlLoadingInfoSecond) < 0);
    }

    @Test
    @DisplayName("если wf_system одинаковые и оба start_running есть, то сравнивает по start_running, а затем по start_loading")
    void sameSystemHaveLoading() {
        CtlLoadingInfo ctlLoadingInfoFirst = result("A-SYSTEM", "2010-01-01 10:00:01.0", "2010-01-01 10:01:00.0");
        CtlLoadingInfo ctlLoadingInfoSecond = result("A-SYSTEM", "2010-01-01 10:00:02.0", "2010-01-01 10:01:00.0");
        assertTrue(ctlLoadingInfoFirst.compareTo(ctlLoadingInfoSecond) < 0);
    }

    private CtlLoadingInfo result(String s, String o, String o2) {
        return new CtlLoadingInfo(null, null, s, null, null, null, o, o2, null, null, null);
    }
}