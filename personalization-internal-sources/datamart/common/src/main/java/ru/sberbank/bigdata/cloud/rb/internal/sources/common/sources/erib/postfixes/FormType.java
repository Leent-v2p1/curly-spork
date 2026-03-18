package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.erib.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.erib.EribInstance;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.erib.EribSourcePostfix;

import java.util.Arrays;

public class FormType extends EribSourcePostfix {

    public FormType(EribInstance instance) {
        super(instance, "form-type", "form-type");
    }

    public static FormType[] values() {
        return Arrays.stream(EribInstance.values())
                .map(FormType::new)
                .toArray(FormType[]::new);
    }
}
