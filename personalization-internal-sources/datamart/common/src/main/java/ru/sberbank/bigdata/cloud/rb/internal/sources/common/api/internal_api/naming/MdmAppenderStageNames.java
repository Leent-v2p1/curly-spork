package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;

import java.util.Optional;

public class MdmAppenderStageNames {

    //EPK FROM EPK TABLES
    public enum EpkSystemsStageTables {
        WAY4("custom_rb_card_stg.epk_id_from_epk_system_stg"),
        LOAN("custom_rb_loan_stg.epk_id_from_epk_system_stg"),
        COD("custom_rb_cod_stg.active_clients_epk_stg_");

        private final String table;

        EpkSystemsStageTables(String table) {
            this.table = table;
        }

        public FullTableName getTable(Optional<SourcePostfix> sourcePostfix) {
            return FullTableName.of(table + sourcePostfix.map(SourcePostfix::getPostfix).orElse(""));
        }
    }
}
