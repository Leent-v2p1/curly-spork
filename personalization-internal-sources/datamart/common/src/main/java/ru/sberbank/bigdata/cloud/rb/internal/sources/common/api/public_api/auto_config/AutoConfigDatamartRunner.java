package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config;

import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteSender;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.auto_config.DatamartIdResolver;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.logging.LoggerTypeId;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.Datamart;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.DatamartRuntimeException;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.SparkSQLUtil;

import java.io.IOException;
import java.util.function.Function;

import static java.lang.Integer.parseInt;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.SysPropertyTool.getSystemProperty;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.SysPropertyTool.safeSystemProperty;

/**
 * Иницилизирует витрину и ее предков необходимыми сущностями и сабмитит ее в DatamartRunner.class
 */
public class AutoConfigDatamartRunner {

    private static Logger log = LoggerFactory.getLogger(AutoConfigDatamartRunner.class);

    private final DatamartIdResolver idResolver;
    private final Function<FullTableName, DatamartServiceFactory> datamartServiceFactoryBuilder;

    public AutoConfigDatamartRunner(DatamartIdResolver idResolver,
                                    Function<FullTableName, DatamartServiceFactory> datamartServiceFactoryBuilder) {
        this.idResolver = idResolver;
        this.datamartServiceFactoryBuilder = datamartServiceFactoryBuilder;
    }

    public static AutoConfigDatamartRunner runner() {
        return new AutoConfigDatamartRunner(new DatamartIdResolver(), DatamartServiceFactory::new);
    }

    public <T extends Datamart> void run(Class<T> datamartClass) {
        final Thread.UncaughtExceptionHandler defaultHandler = Thread.currentThread().getUncaughtExceptionHandler();
        Thread.currentThread().setUncaughtExceptionHandler((Thread t, Throwable e) -> {
                    log.error("Uncaught exception: ", e);
                    defaultHandler.uncaughtException(t, e);
                }
        );
        final FullTableName id = idResolver.resolveId(datamartClass);
        LoggerTypeId.set(id.fullTableName());
        final DatamartServiceFactory datamartServiceFactory = datamartServiceFactoryBuilder.apply(id);
        Environment environment = datamartServiceFactory.parametersService().getEnv();
        cleanupBeforeRun(id, datamartServiceFactory, environment);

        final T datamart = datamartServiceFactory.datamartAutoConfigurer().createDatamart(datamartClass);

        //this try with resources is required because GraphiteSender implements Closeable and should be created only inside try catch
        try (GraphiteSender sender = graphiteSender()) {
            datamartServiceFactory.datamartRunner(sender).run(datamart);
        } catch (IOException e) {
            throw new DatamartRuntimeException("Throwing exception during datamartRunner().run()", e);
        }
    }

    private void cleanupBeforeRun(FullTableName datamartId, DatamartServiceFactory datamartServiceFactory, Environment environment) {
        final SparkSession context = datamartServiceFactory.sqlContext();
        if (environment.isTestEnvironment()) {
            log.warn("Deleting {} table, because current environment is {}", datamartId, environment);
            SparkSQLUtil.dropAndInvalidate(context, datamartId);
        }

        final String reserveFullTableName = datamartServiceFactory.naming().reserveFullTableName();
        SparkSQLUtil.dropAndInvalidate(context, FullTableName.of(reserveFullTableName));
    }

    private Graphite graphiteSender() {
        String hostname = getSystemProperty("spark.graphite.carbon.host");
        if ("".equals(hostname) || hostname == null) {
            log.warn("graphite url is empty or null, so graphite reporting is disabled");
            return null;
        }
        int port = parseInt(safeSystemProperty("spark.graphite.carbon.port"));
        return new Graphite(hostname, port);
    }
}
