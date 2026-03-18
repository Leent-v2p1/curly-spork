package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config;

import org.apache.spark.sql.SparkSession;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.auto_config.ToDeleteResolver;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.CtlApiCalls;

public class ParameterServiceBuilder {
    private FullTableName datamartId;
    private Environment env;
    private ToDeleteResolver toDeleteResolver;
    private CtlApiCalls ctlRestApi;
    private SparkSession sqlContext;

    public static ParameterServiceBuilder builder() {
        return new ParameterServiceBuilder();
    }

    public ParameterServiceBuilder setDatamartId(FullTableName datamartId) {
        this.datamartId = datamartId;
        return this;
    }

    public ParameterServiceBuilder setEnv(Environment env) {
        this.env = env;
        return this;
    }

    public ParameterServiceBuilder setToDeleteResolver(ToDeleteResolver toDeleteResolver) {
        this.toDeleteResolver = toDeleteResolver;
        return this;
    }

    public ParameterServiceBuilder setCtlRestApi(CtlApiCalls ctlRestApi) {
        this.ctlRestApi = ctlRestApi;
        return this;
    }

    public ParameterServiceBuilder setSparkSession(SparkSession sqlContext) {
        this.sqlContext = sqlContext;
        return this;
    }

    public ParametersService build() {
        return new ParametersService(datamartId, env, toDeleteResolver, ctlRestApi, sqlContext);
    }
}
