package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities;

public enum MemoryPreset {

    MINIMAL(MemoryParams.minimalMemoryParams()),
    LOWEST(MemoryParams.lowestPreset()),
    LOW(MemoryParams.lowPreset()),
    MID(MemoryParams.midPreset()),
    HIGH(MemoryParams.highPreset()),
    NOT_SPECIFIED(MemoryParams.midPreset());

    private final MemoryParams params;

    MemoryPreset(MemoryParams params) {
        this.params = params;
    }

    public MemoryParams getParams() {
        return params;
    }
}
