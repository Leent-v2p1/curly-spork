package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

import java.util.Objects;

public abstract class CodSourcePostfix implements SourcePostfix {
    private final String namePostfix;
    private final String path;
    private final CodTerbankCode code;

    public CodSourcePostfix(CodTerbankCode code, String namePostfix, String path) {
        this.code = code;
        this.namePostfix = namePostfix;
        this.path = path;
    }

    @Override
    public String getPostfix() {
        return String.valueOf(code.getCode());
    }

    @Override
    public String getCtlName() {
        return code.getCode() + "-" + namePostfix;
    }

    @Override
    public String getPath() {
        return path;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CodSourcePostfix)) {
            return false;
        }
        CodSourcePostfix that = (CodSourcePostfix) o;
        return Objects.equals(namePostfix, that.namePostfix) &&
                Objects.equals(path, that.path) &&
                code == that.code;
    }

    @Override
    public int hashCode() {
        return Objects.hash(namePostfix, path, code);
    }
}
