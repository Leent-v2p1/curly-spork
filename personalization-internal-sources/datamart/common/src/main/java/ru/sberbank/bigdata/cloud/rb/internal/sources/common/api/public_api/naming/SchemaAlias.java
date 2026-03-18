package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming;

import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.NameAdditions.STG_POSTFIX;

public enum SchemaAlias {
    CUSTOM_RB_SBOL("erib"),
    CUSTOM_RB_SBOL_STG("erib"),

    CUSTOM_RB_VNV("vnv"),
    CUSTOM_RB_VNV_STG("vnv"),

    CUSTOM_RB_EVK("evk"),
    CUSTOM_RB_EVK_STG("evk"),

    public final String schemaAliasValue;

    SchemaAlias(String schemaAliasValue) {
        this.schemaAliasValue = schemaAliasValue;
    }

    public static SchemaAlias of(String schemaName) {
        return SchemaAlias.valueOf(schemaName.toUpperCase());
    }

    public static Optional<SchemaAlias> ofAlias(String aliasToResolve) {
        return getAllPaSchemas().stream()
                .map(SchemaAlias::of)
                .filter(alias -> alias.schemaAliasValue.equals(aliasToResolve))
                .findFirst();
    }

    /**
     * @return the Set of public available schemas. That means schemas that do not contain "stg" in their names
     */

    public static Set<String> getAllPaSchemas() {
        return Arrays.stream(SchemaAlias.values())
                .map(SchemaAlias::name)
                .filter(schema -> !schema.toUpperCase().endsWith(STG_POSTFIX.toUpperCase()))
                .map(String::toLowerCase)
                .collect(Collectors.toSet());
    }

    public static Set<String> getAllSchemaAliases() {
        return Arrays.stream(SchemaAlias.values())
                .map(schema -> schema.schemaAliasValue)
                .filter(schema -> !schema.toUpperCase().endsWith(STG_POSTFIX.toUpperCase()))
                .map(String::toLowerCase)
                .collect(Collectors.toSet());
    }
}
