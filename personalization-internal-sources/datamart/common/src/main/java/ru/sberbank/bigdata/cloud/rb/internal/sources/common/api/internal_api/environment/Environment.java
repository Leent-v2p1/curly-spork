package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Enum используется для 1) получения параметров из правильных разделов datamart-properties.yaml,
 * 2) выбора необходимых действий в коде,
 * 3) подстановки правильных путей в генерируемые артефакты
 * в зависимости от целевого окружения.
 */
public enum Environment {
    PRODUCTION,
    PRODUCTION_TEST;

    private static Environment of(String name) {
        return valueOf(name);
    }

    public static Set<Environment> getEnvs(String[] args) {
        return args.length == 0 ?
                Collections.emptySet() :
                defineEnvs(args);
    }

    public static Set<Environment> defineEnvs(String[] args) {
        Set<String> argsSet = Arrays.stream(args).map(String::toUpperCase).collect(Collectors.toSet());

        Set<String> values = Arrays.stream(values()).map(Environment::name).collect(Collectors.toSet());

        Set<String> intersection = argsSet.stream().filter(values::contains).collect(Collectors.toSet());
        return intersection.stream().map(Environment::of).collect(Collectors.toSet());
    }

    public String nameLowerCase() {
        return super.name().toLowerCase();
    }

    public boolean isTestEnvironment() {
        return this == PRODUCTION_TEST;
    }

    public boolean isProductionEnvironment() {
        return this == PRODUCTION;
    }
}
