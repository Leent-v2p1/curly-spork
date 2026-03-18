package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.runtime.datamart;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.VisibleForTesting;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.DatamartServiceFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.file.HDFSHelper;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.file.PathBuilder;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.runtime.common.SubWfGenerator;

import java.nio.file.Path;
import java.time.LocalDate;

/**
 * Класс для построения витрины по месяцам.
 * Основная задача - создать потоки за каждый месяц, начиная со startDate.
 * Наименование потока витрины вычисляется по наименованию витрины.
 * Наименование потока, который запускает все по месяцам будет следующим:
 * app_path/schema/source_postfix_name/table-name/workflow.xml. Например oozie-app/wf/custom/rb/way4/logonAggrMonthly/agrmnt_mnth/workflow.xml
 */
public abstract class DatamartPeriodGenerator {

    private final FullTableName fullTableName;
    protected final DatamartServiceFactory dsf;
    private final PathBuilder pathBuilder;
    private final String sourcePostfixPath;

    public DatamartPeriodGenerator(FullTableName fullTableName, DatamartServiceFactory serviceFactory, String sourcePostfixPath) {
        this.fullTableName = fullTableName;
        this.dsf = serviceFactory;
        this.pathBuilder = new PathBuilder(dsf.parametersService().getEnv());
        this.sourcePostfixPath = sourcePostfixPath;
    }

    public void generateAndWriteHDFS() {
        final String oozieWf = generate();
        writeToHdfs(oozieWf);
    }

    @VisibleForTesting
    protected String generate() {
        final Environment env = dsf.parametersService().getEnv();
        final LocalDate buildDate = dsf.parametersService().ctlBuildDate();
        final boolean firstBuild = dsf.parametersService().isFirstLoading();

        final String tableName = (env.isTestEnvironment() ? "test_" : "") + fullTableName.tableName();
        String fullAppPath = pathBuilder.resolveGeneratedWorkflow(fullTableName);
        final SubWfGenerator subWfGenerator = new SubWfGenerator(
                tableName,
                fullAppPath,
                getStartDate(firstBuild, buildDate),
                buildDate,
                firstBuild,
                env);

        return subWfGenerator.generate();
    }

    private void writeToHdfs(String oozieWf) {
        final Path path = pathBuilder.resolveMonthPeriodDatamart(fullTableName, sourcePostfixPath);
        HDFSHelper.rewriteFile(oozieWf, path);
    }

    protected abstract LocalDate getStartDate(boolean firstBuild, LocalDate buildDate);
}
