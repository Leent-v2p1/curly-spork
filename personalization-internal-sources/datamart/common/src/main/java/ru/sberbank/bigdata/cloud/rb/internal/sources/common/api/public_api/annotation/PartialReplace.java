package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation;

import org.apache.commons.lang.StringUtils;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.save_remover.IncrementSaveRemover;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_remover.EmptySaveRemover;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation used for non-historical partitioned datamarts
 * #partitioning - consume one column name for dynamic partitioning
 * other partitioning options can be presents by #customSavingStrategy()
 * by default present datamart without partitioning
 * #saveRemover - consume #IncrementSaveRemover .class
 * by default present datamart without IncrementSaveRemover
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface PartialReplace {
    String partitioning() default StringUtils.EMPTY;

    Class<? extends IncrementSaveRemover> saveRemover() default EmptySaveRemover.class;
}
