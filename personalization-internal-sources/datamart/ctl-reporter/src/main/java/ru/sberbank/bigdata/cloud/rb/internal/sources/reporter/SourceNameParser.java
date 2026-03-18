package ru.sberbank.bigdata.cloud.rb.internal.sources.reporter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SourceNameParser {

    /**
     * Регулярное выражение, которое разбивает имя потока на именованные группы, из которых в дальнейшем строится имя системы
     * Примеры:
     *
     * Дано: wfName = masspers-clsklrozn-erib-ikfl3-oper-ahd-datamarts
     * Полученные именованныег групы: test = "", source = "erib", terbank = "", ikfl = "ikfl3", postfix = "oper-ahd"
     *
     * Дано: wfName = masspers-clsklrozn-test-cod-49-monthly-datamarts
     * Полученные именованныег групы: test = "test", source = "cod", terbank = "49", ikfl = "", postfix = ""
     */
    private Pattern wfNamePattern = Pattern.compile(
            "(?:masspers-[a-z]+-)" +                   //  masspers-(имя стенда)-
                    "(?<test>test|)-?" +               // 'test-' или ничего
                    "(?<source>[a-z_]+\\d?)" +         // название системы, включая '_' и 0 или 1 цифру на конце
                    "(?<terbank>-\\d{2,3}|)" +         // номер (тербанка 2 цифры) или ничего
                    "(?<ikfl>-ikfl[1-5_gf]{0,3}|)-?" + // ikfl с 1 по 5 и gf или ничего
                    "(?<postfix>[a-z-]+|)-" +          // постфикс любой длины включая '-' или ничего
                    "(datamarts|streaming)"            // datamart или streaming на конце
    );

    public String resolveWfSystem(String wfName) {
        if (wfName.startsWith("infa.ap_cg_gMainHDP.masspers-clsklrozn")) {
            return "Erib2AHD";
        }
        Matcher matcher = wfNamePattern.matcher(wfName);
        return matcher.find() ? resolveName(matcher) : "undefined";
    }

    private String resolveName(Matcher matcher) {
        String test = matcher.group("test");
        String source = matcher.group("source");
        String terbank = matcher.group("terbank");
        String ikfl = matcher.group("ikfl");
        String postfix = matcher.group("postfix").contains("ahd") ? "-ahd" : ikfl;
        source = (source.equals("rb") || source.equals("risk")) ? "triggers" : matcher.group("source");
        return test.isEmpty()
                ? source + terbank + postfix
                : test;
    }
}
