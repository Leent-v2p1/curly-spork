package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.erib;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

import java.util.Objects;

public abstract class EribSourcePostfix implements SourcePostfix {
    private final String namePostfix;
    private final String path;
    private final EribInstance instance;

    public EribSourcePostfix(EribInstance instance, String namePostfix, String path) {
        this.instance = instance;
        this.namePostfix = namePostfix;
        this.path = path;
    }

    @Override
    public String getPostfix() {
        return instance.name().toLowerCase();
    }

    @Override
    public String getCtlName() {
        return instance.name().toLowerCase() + "-" + namePostfix;
    }

    @Override
    public String getPath() {
        return path;
    }

    public int code() {
        return instance.code();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof EribSourcePostfix)) {
            return false;
        }
        EribSourcePostfix that = (EribSourcePostfix) o;
        return Objects.equals(namePostfix, that.namePostfix) &&
                Objects.equals(path, that.path) &&
                instance == that.instance;
    }

    @Override
    public int hashCode() {
        return Objects.hash(namePostfix, path, instance);
    }
}
