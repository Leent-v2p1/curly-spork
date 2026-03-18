package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.DatamartContext;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.SparkSQLUtil;

import java.util.List;

public class CodDatamartsMerger {

    private final DatamartContext dc;
    private final List<CodTerbankCode> codTerbankCodes;

    public CodDatamartsMerger(DatamartContext dc, List<CodTerbankCode> codTerbankCodes) {
        this.dc = dc;
        this.codTerbankCodes = codTerbankCodes;
    }

    public CodDatamartsMerger(DatamartContext dc) {
        this.dc = dc;
        this.codTerbankCodes = CodTerbankCode.listOfValues();
    }

    public Dataset<Row> merge(String tableWithoutTerbank) {
        Dataset<Row>[] tables =
                codTerbankCodes.stream()
                        .map(codTerbankCode -> tableWithoutTerbank + codTerbankCode.getCode())
                        .map(dc::targetTable)
                        .toArray(Dataset[]::new);
        return SparkSQLUtil.unionAll(tables);
    }
}
