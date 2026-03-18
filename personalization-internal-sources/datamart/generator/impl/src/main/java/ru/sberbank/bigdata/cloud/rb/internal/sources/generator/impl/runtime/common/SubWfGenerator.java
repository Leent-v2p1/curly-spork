package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.runtime.common;

import freemarker.template.Template;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.FreemarkerHelper;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.WfEntity;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.date.DateHelper.MONTH_FORMAT;

public class SubWfGenerator {

    private static final Logger log = LoggerFactory.getLogger(SubWfGenerator.class);
    public static final int MONTHS_FOR_TEST_BUILD = 2;

    private final String tableName;
    private final String appPath;
    private final String prefix;
    private final LocalDate startDate;
    private final LocalDate currentMonth;
    private final boolean firstBuild;
    private final Environment env;

    public SubWfGenerator(String tableName,
                          String appPath,
                          LocalDate startDate,
                          LocalDate currentDate,
                          boolean firstBuild,
                          Environment env) {
        this.tableName = tableName;
        this.appPath = appPath;
        this.prefix = "create-" + tableName + "-";
        this.startDate = LocalDate.of(startDate.getYear(), startDate.getMonth(), 1);
        this.currentMonth = LocalDate.of(currentDate.getYear(), currentDate.getMonth(), 1);
        this.firstBuild = firstBuild;
        this.env = env;
    }

    public String generate() {
        String result = generateOozieWf(mapToWfEntities());
        log.info(result);
        return result;
    }

    Map<String, Object> mapToWfEntities() {
        long numberOfMonths = env.isTestEnvironment() ? MONTHS_FOR_TEST_BUILD : ChronoUnit.MONTHS.between(startDate, currentMonth);
        LocalDate previousMonth = currentMonth.minusMonths(1L);

        List<WfEntity> wfs = Stream
                .iterate(startDate, date -> date.plusMonths(1L))
                .limit(numberOfMonths)
                .map((LocalDate dateForWf) -> {
                    boolean isLastMonthForTestBuild = dateForWf.isEqual(startDate.plusMonths(MONTHS_FOR_TEST_BUILD - 1L));
                    boolean isLast = env == Environment.PRODUCTION_TEST ? isLastMonthForTestBuild : dateForWf.isEqual(previousMonth);
                    boolean isFirst = firstBuild && dateForWf.equals(startDate);
                    return createWf(dateForWf, isLast, isFirst);
                })
                .collect(Collectors.toList());

        Map<String, Object> mapping = new HashMap<>();
        mapping.put("wfs", wfs);
        mapping.put("firstWf", prefix + startDate.format(MONTH_FORMAT));
        mapping.put("tableName", tableName);
        return mapping;
    }

    private String generateOozieWf(Map<String, Object> mapping) {
        Template template = FreemarkerHelper.getTemplate("sub-wf.ftl");
        return FreemarkerHelper.printTemplate(template, mapping);
    }

    private WfEntity createWf(LocalDate date, boolean isLast, boolean firstBuild) {
        String name = prefix + date.format(MONTH_FORMAT);
        String nextWf = isLast ? "end" : prefix + date.plusMonths(1L).format(MONTH_FORMAT);
        String dateValue = date.format(DateTimeFormatter.ISO_LOCAL_DATE);
        return new WfEntity(name, appPath, nextWf, firstBuild, dateValue);
    }
}
