package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.base.WorkflowType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;

/**
 * Class contains method that parse properties file map to get action of specific type
 */
public class PropertiesUtils {

    private static final Logger log = LoggerFactory.getLogger(PropertiesUtils.class);

    public static Set<String> getAllActionsIdWithType(WorkflowType type) {
        return getAllActionsIdWithType(type, null);
    }

    public static Set<String> getAllActionsIdWithType(WorkflowType type, @Nullable Map<String, Object> properties) {
        final Map<String, Object> finalProps = Optional.ofNullable(properties).orElseGet(Properties::get);
        return getAllActionsIdStream(finalProps)
                .filter(
                        datamartId -> {
                            String workflowType = (String) finalProps.get(datamartId + ".type");
                            checkTypeValid(workflowType, datamartId);
                            return type.getKey().equals(workflowType);
                        }
                )
                .collect(toSet());
    }

    public static Set<String> getAllActionsId() {
        return getAllActionsId(null);
    }

    public static Set<String> getAllActionsId(@Nullable Map<String, Object> properties) {
        properties = Optional.ofNullable(properties).orElseGet(Properties::get);
        return getAllActionsIdStream(properties)
                .collect(toSet());
    }

    private static Stream<String> getAllActionsIdStream(@Nonnull Map<String, Object> properties) {
        return properties
                .keySet()
                .stream()
                .filter(key -> !key.startsWith("global."))
                .map(key -> {
                    String[] split = key.split("\\.");
                    return split[0] + "." + split[1];
                });
    }

    private static void checkTypeValid(String workflowType, String datamartId) {
        boolean isTypeValid = Arrays.stream(WorkflowType.values())
                .anyMatch(anObject -> anObject.getKey().equals(workflowType));
        if (!isTypeValid && !datamartId.endsWith("_hst")) {
            log.info("Datamart with id = {} of unsupported type = {}", datamartId, workflowType);
        }
    }
}
