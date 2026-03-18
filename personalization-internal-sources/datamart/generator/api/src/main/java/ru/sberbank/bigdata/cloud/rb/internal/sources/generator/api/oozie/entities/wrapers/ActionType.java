package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.wrapers;

/**
 * Created by SBT-Yakovlev-AV on 19.09.2017.
 */
public enum ActionType {

    SPARK("spark"), HIVE("hive"), JAVA("java"), NOT_ACTION("not_action");

    private String key;

    ActionType(String type) {
        this.key = type;
    }

    public String getKey() {
        return key;
    }
}
