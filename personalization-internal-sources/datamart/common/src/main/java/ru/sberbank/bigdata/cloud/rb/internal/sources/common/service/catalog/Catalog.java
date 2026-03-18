package ru.sberbank.bigdata.cloud.rb.internal.sources.common.service.catalog;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.SchemaNames.*;

//updated txn_pmt_reg_type.csv file
public enum Catalog implements FullTableName {
    TRANS_TYPE(CARD.resolve("trans_type")),
    AGREEMENT_STATUS(CARD_STG.resolve("agreement_status")),
    CARD_STATUS(CARD_STG.resolve("card_status")),
    ref_card_reasons(CARD_STG.resolve("ref_card_reasons")),
    REF_TXN_TYPE(CARD_STG.resolve("ref_txn_type")),
    REF_SECOND_TXN_TYPE(CARD_STG.resolve("ref_second_txn_type")),
    REF_REGION_TB(CARD_STG.resolve("ref_region_tb")),
    TXN_PMT_REG_TYPE(COD.resolve("txn_pmt_reg_type")),
    TERBANK(COD.resolve("terbank")),
    DEP_TYPE(COD.resolve("dep_type")),
    INTRST_TYPE(COD.resolve("intrst_type")),
    TXN_COD_TYPE(COD.resolve("txn_cod_type")),
    CAP_TYPE(COD.resolve("cap_type")),
    COD_DIC_QVB(COD.resolve("cod_dic_qvb")),
    TYPE_GROUP(COD.resolve("ref_chnl_type_group")),
    GRANT_OPER(COD.resolve("grant_oper")),
    TARIFF_PLAN_CAMPAIGN(COD.resolve("tariff_plan_campaign")),
    REF_MCC(CARD_STG.resolve("ref_mcc")),

    REFUSAL_TYPE(STOPLIST_CR.resolve("refusal_type")),

    COGNITION_CATEGORIES(YANDEX_MARKET.resolve("cognition_categories")),

    MCC_CODE(SMARTVISTA.resolve("mcc_code")),
    BIN_TOKEN(SMARTVISTA.resolve("bin_token")),

    MAPPING_DOMAIN_TO_CATEGORY(CLICKSTREAM.resolve("mapping_domain_to_category"));

    private final FullTableName fullTableName;

    Catalog(FullTableName fullTableName) {
        this.fullTableName = fullTableName;
    }

    @Override
    public String dbName() {
        return fullTableName.dbName();
    }

    @Override
    public String tableName() {
        return fullTableName.tableName();
    }

    @Override
    public String fullTableName() {
        return fullTableName.fullTableName();
    }

    @Override
    public String toString() {
        return fullTableName();
    }

    public static Set<String> getCatalogSchemas(){
        return Arrays.stream(Catalog.values())
                .map(Catalog::dbName)
                .collect(Collectors.toSet());
    }
}
