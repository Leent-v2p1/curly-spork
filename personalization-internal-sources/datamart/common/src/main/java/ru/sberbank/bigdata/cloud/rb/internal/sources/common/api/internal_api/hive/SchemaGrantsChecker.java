package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.hive;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.SparkSQLUtil;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static java.lang.System.lineSeparator;

/**
 * Class creates hive jdbc connection to hive server, retrieves all available schema with commands:
 * - SHOW CURRENT ROLES
 * - SHOW GRANT ROLE role_name
 * cache it, and then check if required table's schema is in that list.
 * If one of user roles has '*' grants, method would return true
 */
public class SchemaGrantsChecker {

    private static final Logger log = LoggerFactory.getLogger(SchemaGrantsChecker.class);

    private final String user;
    private final String hdpType;
    private final HiveDao hiveDao;
    private final UserGroupInformation ugi;
    private Set<String> objectsWithAccess = null;

    public SchemaGrantsChecker(HiveDao hiveDao, UserGroupInformation ugi, String user, String hdpType) {
        this.hiveDao = hiveDao;
        this.ugi = ugi;
        this.user = user;
        this.hdpType = hdpType;
    }

    public void checkGrantsToSchemaLocation(SparkSession context, String schema) {
        String location = SparkSQLUtil.databaseLocation(context, schema);
        checkAccessForObject(location);
    }

    /**
     * В Sentry хранятся вперемешку доступные схемы и пути к ним.
     * То есть в одной колонке рядом лежат custom_rb_loan и hdfs://ns1/data/custom/rb/loan
     * Этот метод позволяет проверить доступность любого объекта, без разделения на схему и путь.
     *
     * @param objectToCheck - схема или путь для проверки доступности
     */
    public void checkAccessForObject(String objectToCheck) {
        if (objectsWithAccess == null) {
                objectsWithAccess = getSchemasAndPathsWithAccess();
            log.warn("objectWithAccess: {}", objectsWithAccess);
        }
        log.info("Available schemas and paths: {}", objectsWithAccess);
        if (objectsWithAccess.contains(objectToCheck)) {
            log.info("User {} has access to {}", user, objectToCheck);
        } else if (objectsWithAccess.contains("sdp")) {
            log.info("This is SDP User {} disabling check access to {}", user, objectToCheck);
        }
        else {
            log.error("User has no access to {}", objectToCheck);
            throw new IllegalStateException("User has no access to " + objectToCheck);
        }
    }

    public void checkAccess(FullTableName fullName) {
        checkAccessForObject(fullName.dbName());
    }

    private Set<String> getSchemasAndPathsWithAccess() {
        log.info("for Debug hdpType: {}", hdpType);
        try {
            @SuppressWarnings("unchecked")
            Set<String> retrievedSchemas = (Set<String>) ugi.doAs((PrivilegedExceptionAction<Object>) () -> {
                if (hdpType.equals("sdp")) {
                    hiveDao.connect();
                    Set<String> schemas = new HashSet<>(Arrays.asList("sdp"));
                    return schemas;
                } else {
                    hiveDao.connect();
                    Set<String> schemas = hiveDao.select("SHOW CURRENT ROLES", "role").stream()
                            .flatMap(role -> hiveDao.select("SHOW GRANT ROLE " + role, "database").stream())
                            .collect(Collectors.toSet());

                    final String schemasList = schemas.stream().collect(Collectors.joining(lineSeparator()));
                    log.debug("Retrieved grants to schemas: {}", schemasList);
                    return schemas;
                }
            });
            log.info("retrievedSchemas: {}", retrievedSchemas);
            return retrievedSchemas;
        } catch (IOException e) {
            throw new UncheckedIOException("Error during process of retrieving list of available schemas to user " + user, e);
        } catch (InterruptedException e) {
            log.warn("Thread interrupted during getSchemasAndPathsWithAccess()!", e);
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Failed to get schema, caused by interruption");
        } finally {
            hiveDao.disconnect();
        }
    }
}
