package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public enum CtlCategory {
    PERSONALIZATION_INTERNAL("DHP2 Personalization internal"),
    PERSONALIZATION_INTERNAL_SUPPORT("Personalization internal support"),
    PERSONALIZATION_INTERNAL_VERIFICATION("Personalization internal verification"),
    PERSONALIZATION_INTERNAL_DEVELOP("Personalization internal develop"),
    PERSONALIZATION_INTERNAL_UAT("Personalization internal uat"),
    PERSONALIZATION_INTERNAL_INTEGRATION("Personalization internal integration"),
    PERSONALIZATION_INTERNAL_INTEGRATION_SDS("Personalization internal integration sds");

    private final String value;

    CtlCategory(String value) {
        this.value = value;
    }

    public static Set<String> setOfValues() {
        return Arrays.stream(CtlCategory.values()).map(CtlCategory::value).collect(Collectors.toSet());
    }

    public String value() {
        return value;
    }
}
