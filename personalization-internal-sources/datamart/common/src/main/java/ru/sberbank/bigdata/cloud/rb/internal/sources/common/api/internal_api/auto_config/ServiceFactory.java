package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.auto_config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.columnar.STRING;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.hive.HiveDao;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.hive.SchemaGrantsChecker;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.VisibleForTesting;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.SysPropertyTool;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.LocalDate;
import java.util.Map;
import java.util.Optional;
import java.io.File;

import static java.util.Collections.emptyMap;

public class ServiceFactory {

    private static final Logger log = LoggerFactory.getLogger(ServiceFactory.class);
    protected final Map<String, String> sparkContextProperties;
    protected final Map<String, String> sparkContextLocalProperties;
    private final String jobName;
    private SparkSession sqlContext;
    private SchemaGrantsChecker grantsChecker;

    public ServiceFactory(String jobName) {
        this(jobName, emptyMap(), emptyMap());
    }

    public ServiceFactory(String jobName, Map<String, String> sparkContextProperties, Map<String, String> sparkContextLocalProperties) {
        this.jobName = jobName;
        this.sparkContextProperties = sparkContextProperties;
        this.sparkContextLocalProperties = sparkContextLocalProperties;
    }

    public SparkSession sqlContext() {
        String idLoading = SysPropertyTool.getSystemProperty("spark.ctl.loading.id","hdp2");
        if (sqlContext == null) {
            String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
            final SparkSession session = SparkSession.builder()
                    .appName("loading_id=" + idLoading+ ":wf_name="+ jobName)
                    .config("spark.sql.crossJoin.enabled", "true")
                    .config("spark.sql.warehouse.dir", warehouseLocation)
                    .enableHiveSupport()
                    .getOrCreate();
            final RuntimeConfig conf = session.conf();
//            sparkContextProperties.forEach(conf::set);
            SparkContext sc = session.sparkContext();
//            sparkContextLocalProperties.forEach(sc::setLocalProperty);
            this.sqlContext = session;
        }
        return sqlContext;
    }

    public SchemaGrantsChecker schemaGrantsChecker() {
        if (grantsChecker == null) {
            String principal = SysPropertyTool.safeSystemProperty("spark.principal");
            String hiveUrl = SysPropertyTool.safeSystemProperty("spark.hiveJdbcUrl");
            String hdpType = SysPropertyTool.getSystemProperty("spark.hdpType", "hdp");
            String jdbcUrl = hiveUrl + "/default;principal=" + principal;
            if (hdpType.equals("sdp")) {
                log.info("HDP TYPE SDP !!!!!!!!");
                jdbcUrl = hiveUrl + "/default;principal=" + principal + ";serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2";
            }
            log.info("Jdbc for hdpType: {} url for access check: '{}'", hdpType, jdbcUrl);
            String userName = SysPropertyTool.safeSystemProperty("spark.user.name");
            String keyTabPath = SysPropertyTool.safeSystemProperty("spark.keytabPath");

            log.info("keyTab path: {}", keyTabPath);
            String keytab = keyTabPath.replace("/keytab/", "");
            log.info("keyTab path: {}", keytab);

            String server = principal.split("@")[1];
            String fullUserName = userName + "@" + server;
            log.info("FullUserName for access check: '{}'", fullUserName);
            Configuration conf = new Configuration();
            conf.set("hadoop.security.authentication", "Kerberos");
            UserGroupInformation ugi = getUserGroupInformation(keytab, fullUserName, conf);
            this.grantsChecker = new SchemaGrantsChecker(new HiveDao(jdbcUrl), ugi, fullUserName, hdpType);
            log.warn("grantsChecker: {}", grantsChecker);
        }
        return grantsChecker;
    }

    @VisibleForTesting
    UserGroupInformation getUserGroupInformation(String keytab, String fullUserName, Configuration conf) {
        try {
            UserGroupInformation.setConfiguration(conf);
            return UserGroupInformation.loginUserFromKeytabAndReturnUGI(fullUserName, keytab);
        } catch (IOException e) {
            throw new UncheckedIOException("Exception during loginUserFromKeytabAndReturnUGI()", e);
        }
    }

    public Optional<LocalDate> recoveryDate() {
        return Optional.ofNullable(SysPropertyTool.getSystemProperty("spark.recovery.date"))
                .filter(s -> !s.isEmpty())
                .map(LocalDate::parse);
    }
}
