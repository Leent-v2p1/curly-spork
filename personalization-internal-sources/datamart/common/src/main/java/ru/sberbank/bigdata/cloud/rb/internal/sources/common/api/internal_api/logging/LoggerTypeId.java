package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.logging;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;

public class LoggerTypeId {
    public static void set(String datamartId) {
        System.setProperty("ru.sberbank.cm.flumeappender.dynamic.type_id", datamartId);
    }

    public static void set(FullTableName datamartId) {
        LoggerTypeId.set(datamartId.fullTableName());
    }
}
