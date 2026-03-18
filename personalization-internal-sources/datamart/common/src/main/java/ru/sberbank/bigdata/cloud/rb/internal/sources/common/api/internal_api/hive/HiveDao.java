package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.hive;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.DatamartRuntimeException;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;

public class HiveDao implements Dao {

    private static final Logger log = LoggerFactory.getLogger(HiveDao.class);

    private final String jdbcUrl;

    private Connection connection;

    public HiveDao(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        } catch (ClassNotFoundException ex) {
            throw new DatamartRuntimeException("Cannot initialize HiveDriver ", ex);
        }
    }

    @Override
    public void connect() {
        if (!hasConnection()) {
            try {
                connection = DriverManager.getConnection(jdbcUrl);
            } catch (SQLException ex) {
                throw new DatamartRuntimeException(ex);
            }
        }
    }

    @Override
    public boolean executeSql(String sql) {
        if (!hasConnection()) {
            throw new IllegalStateException("Open connection before execute sql");
        }
        try (Statement statement = connection.createStatement()) {
            return statement.execute(sql);
        } catch (SQLException ex) {
            throw new DatamartRuntimeException("Error while executing sql `" + sql + "';", ex);
        }
    }

    @Override
    public Collection<String> select(String sql, String colName) {
        Collection<String> result;
        if (!hasConnection()) {
            throw new IllegalStateException("Open connection before execute sql");
        }
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            result = new ArrayList<>();
            while (resultSet.next()) {
                result.add(resultSet.getString(colName));
            }
        } catch (SQLException e) {
            throw new DatamartRuntimeException(e);
        }
        return result;
    }

    @Override
    public void disconnect() {
        if (hasConnection()) {
            try {
                connection.close();
            } catch (SQLException ex) {
                log.error("exception while closing the hive jdbc connection ", ex);
            }
        }
    }

    private boolean hasConnection() {
        if (connection != null) {
            try {
                return !connection.isClosed();
            } catch (SQLException ex) {
                log.error("exception while checking if connection is closed ", ex);
            }
        }
        return false;
    }
}
