package ru.sberbank.bigdata.cloud.rb.internal.sources.common.util;

import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.StringUtils.toInt;

public class SysPropertyTool {

    private static final Logger log = LoggerFactory.getLogger(SysPropertyTool.class);

    public static String getSystemProperty(String key) {
        String value = System.getProperty(key);
        log.info("Got system property: {} = {}", key, value);
        return value;
    }

    public static String getSystemProperty(String key, String defaultValue) {
        String value = getSystemProperty(key);
        return (value == null) ? defaultValue : value;
    }

    public static Optional<Integer> getIntSystemProperty(String key) {
        try {
            return Optional.of(toInt(getSystemProperty(key)));
        } catch (NumberFormatException | NullPointerException e) {
            return Optional.empty();
        }
    }

    public static String safeSystemProperty(String key) {
        return safeSystemProperty(key, "System property: " + key + " must be non empty");
    }

    public static String safeSystemProperty(String key, String message) {
        String value = getSystemProperty(key);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(message);
        }
        return value;
    }

    public static String getUserHomePath() throws IOException {
        final UserGroupInformation ugi = UserGroupInformation.getLoginUser();
        return "/user/" + ugi.getShortUserName();
    }

    public static String getUserHomePathChecked() {
        try {
            return getUserHomePath();
        } catch (IOException e) {
            log.warn("Cannot get login user.", e);
            throw new IllegalStateException("Cannot get login user.", e);
        }
    }
}
