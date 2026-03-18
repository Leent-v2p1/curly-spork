package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.save_remover;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.PartialReplace;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_remover.EmptySaveRemover;

/**
 * Returns instance of IncrementSaveRemover based on the certain type of annotation in a specific datamart
 */
public class IncrementSaveRemoverHandler {
    private static final Logger log = LoggerFactory.getLogger(IncrementSaveRemoverHandler.class);

    public IncrementSaveRemover getIncrementSaveRemover(String className) {
        try {
            final Class<?> datamartClass = Class.forName(className);
            final PartialReplace annotation = datamartClass.getAnnotation(PartialReplace.class);
            if (annotation == null || annotation.saveRemover().equals(EmptySaveRemover.class)) {
                log.info("IncrementSaveRemover for class {} not found", className);
                return null;
            }
            final Class<? extends IncrementSaveRemover> aClass = annotation.saveRemover();
            final IncrementSaveRemover incrementSaveRemover = aClass.newInstance();
            log.info("IncrementSaveRemover for class {} loaded: {}", className, incrementSaveRemover.getClass().getName());
            return incrementSaveRemover;
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            throw new IncrementSaveRemoverLoadException(e);
        }
    }
}
