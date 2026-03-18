package ru.sberbank.bigdata.cloud.rb.internal.sources.common.datamart_properties_validators;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.source.workflow.*;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.write.WorkflowAppXmlUnMarshaller;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.util.FileUtils;

import javax.xml.XMLConstants;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.EntitiesYamlIsValidTest.ETL_DIR;

public class OozieValidation {

    private static final Logger log = LoggerFactory.getLogger(OozieValidation.class);
    public static final PathMatcher ERIB_BY_INSTANCE_DAILY_WF = FileSystems.getDefault()
            .getPathMatcher("glob:**/erib/workflows/byInstance-daily/workflow.xml");
    public static final PathMatcher COD_DEP_AGRMNT_DAILY_WF = FileSystems.getDefault()
            .getPathMatcher("glob:**/cod/workflows/depAgrmntDaily/workflow.xml");
    public static final PathMatcher COD_DEP_AGRMNT_AGGR_WF = FileSystems.getDefault()
            .getPathMatcher("glob:**/cod/workflows/depAgrmntAggr/workflow.xml");
    public static final PathMatcher COD_PAYMENT_DAILY_WF = FileSystems.getDefault()
            .getPathMatcher("glob:**/cod/workflows/paymentDaily/workflow.xml");
    public static final PathMatcher COD_TARIFF_WF = FileSystems.getDefault()
            .getPathMatcher("glob:**/cod/workflows/tariffDaily/workflow.xml");
    public static final PathMatcher COD_TESTING_BY_INSTANCE_WF = FileSystems.getDefault()
            .getPathMatcher("glob:**/cod/workflows/testingByInstance/workflow.xml");
    public static final PathMatcher COD_TXN_DAILY_WF = FileSystems.getDefault()
            .getPathMatcher("glob:**/cod/workflows/txnDaily/workflow.xml");
    public static final List<PathMatcher> EXCEPTIONS = Arrays.asList(
            ERIB_BY_INSTANCE_DAILY_WF,
            COD_DEP_AGRMNT_DAILY_WF,
            COD_PAYMENT_DAILY_WF,
            COD_TARIFF_WF,
            COD_DEP_AGRMNT_AGGR_WF,
            COD_TXN_DAILY_WF,
            COD_TESTING_BY_INSTANCE_WF);

    @Test
    @DisplayName("проверяет, что все ok='...' существуют, а error='...' идут в join или kill")
    void validateOozieWorkflow() {
        final WorkflowAppXmlUnMarshaller workflowAppXmlUnMarshaller = new WorkflowAppXmlUnMarshaller();

        getXmlToCheck()
                .forEach(file -> {
                    final String content = FileUtils.readAsString(file);
                    final WORKFLOWAPP unmarshall = workflowAppXmlUnMarshaller.unmarshall(content);
                    final START start = unmarshall.getStart();
                    final List<Object> decisionOrForkOrJoin = unmarshall.getDecisionOrForkOrJoin();
                    final Map<String, ActionContainer> actionsMap = fillActionMap(decisionOrForkOrJoin);
                    walkWorkflow(start, actionsMap, file);
                });
    }

    @Test
    void validateXSDSchema() throws SAXException {
        final File xsd = new File("../common/src/main/resources/oozie/generation/oozie-workflow-0.4.xsd");
        final SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        final Validator validator = factory.newSchema(xsd).newValidator();

        getXmlToCheck()
                .forEach(path -> {
                    try {
                        validator.validate(new StreamSource(path.toFile()));
                    } catch (SAXException | IOException e) {
                        throw new IllegalStateException("Error during validation of XML file " + path, e);
                    }
                });
    }

    private Stream<Path> getXmlToCheck() {
        return Stream.of(Objects.requireNonNull(ETL_DIR.toFile().listFiles()))
                .filter(File::isDirectory)
                .map(file -> new File(file, "src/main/resources/oozie/production/oozie-app/wf/custom/rb/production/"))
                .map(this::getOneDirectory)
                .map(file -> new File(file, "workflows"))
                .flatMap(file -> Arrays.stream(Objects.requireNonNull(file.listFiles())))
                .map(file -> new File(file, "workflow.xml"))
                .map(File::toPath);
    }

    private File getOneDirectory(File file) {
        final File[] files = file.listFiles();
        if (files == null || files.length != 1) {
            throw new IllegalStateException("Not exactly 1 file in " + file);
        } else {
            return files[0];
        }
    }

    private Map<String, ActionContainer> fillActionMap(List<Object> decisionOrForkOrJoin) {
        Map<String, ActionContainer> actionsMap = new HashMap<>();
        decisionOrForkOrJoin.forEach(o -> {
            final String actionName;
            String ok;
            String fail;
            if (o instanceof KILL) {
                final KILL kill = (KILL) o;
                actionName = kill.getName();
                actionsMap.put(actionName, new ActionContainer(null, null));
            } else if (o instanceof FORK) {
                FORK fork = (FORK) o;
                actionName = fork.getName();
                final List<FORKTRANSITION> path = ((FORK) o).getPath();
                final List<String> stringList = path.stream().map(FORKTRANSITION::getStart).collect(Collectors.toList());
                actionsMap.put(actionName, new ActionContainer(null, null, stringList));
            } else if (o instanceof DECISION) {
                DECISION decision = (DECISION) o;
                actionName = decision.getName();
                ok = decision.getSwitch().getDefault().getTo();
                fail = decision.getSwitch().getCase().get(0).getTo();
                actionsMap.put(actionName, new ActionContainer(ok, fail));
            } else if (o instanceof JOIN) {
                JOIN join = (JOIN) o;
                actionName = join.getName();
                ok = join.getTo();
                actionsMap.put(actionName, new ActionContainer(ok, null));
            } else if (o instanceof ACTION) {
                ACTION action = (ACTION) o;
                actionName = action.getName();
                ok = action.getOk().getTo();
                fail = action.getError().getTo();
                actionsMap.put(actionName, new ActionContainer(ok, fail));
            } else {
                throw new IllegalStateException("Unexpected action type " + o);
            }
        });
        return actionsMap;
    }

    private void walkWorkflow(START start, Map<String, ActionContainer> actionsMap, Path file) {
        final Deque<String> forks = new LinkedList<>();
        final String currentAction = start.getTo();
        walk(currentAction, actionsMap, forks, file);
    }

    public void walk(String name, Map<String, ActionContainer> actionsMap, Deque<String> forks, Path file) {
        if (name.equals("end")) {
            return;
        }
        final ActionContainer actionContainer = actionsMap.get(name);
        if (actionContainer == null) {
            throw new IllegalStateException("Failed to find " + name + " in file " + file);
        }
        if (actionContainer.isFork()) {
            forks.push(name);
            actionContainer.fork.forEach(workflowInFork -> walk(workflowInFork, actionsMap, new LinkedList<>(forks), file));
        } else if (actionContainer.isJoin()) {
            forks.pop();
            walk(actionContainer.ok, actionsMap, forks, file);
        } else {
            if (checkErrorBranchCorrect(name, actionsMap, forks, file, actionContainer)) {
                return;
            }
            walk(actionContainer.ok, actionsMap, forks, file);
        }
    }

    private boolean checkErrorBranchCorrect(String name,
                                            Map<String, ActionContainer> actionsMap,
                                            Deque<String> forks,
                                            Path file,
                                            ActionContainer actionContainer) {
        if (!actionContainer.fail.equals("kill")) {
            if (!forks.isEmpty()) {
                final ActionContainer ac = actionsMap.get(actionContainer.fail);
                if (ac == null) {
                    throw new IllegalStateException("Error points to '" + actionContainer.fail + "' in action: " + name + "in file %s");
                }
                if (!ac.isJoin()) {
                    final String exceptionMessage = String.format(
                            "Error points to '%s' in action: %s but should point to join, because inside fork %s in file %s",
                            actionContainer.fail, name, forks.peek(), file);
                    handle(exceptionMessage, file);
                }
            } else {
                final String exceptionMessage = String.format(
                        "Error points to '%s' in action: %s in file %s",
                        actionContainer.fail, name, file);
                handle(exceptionMessage, file);
            }
        }
        return false;
    }

    private void handle(String exceptionMessage, Path file) {
        if (EXCEPTIONS.stream().anyMatch(pathMatcher -> pathMatcher.matches(file))) {
            log.warn(exceptionMessage);
        } else {
            throw new IllegalStateException(exceptionMessage);
        }
    }

    static class ActionContainer {
        String ok;
        String fail;
        List<String> fork;

        public ActionContainer(String ok, String fail) {
            this.ok = ok;
            this.fail = fail;
        }

        public ActionContainer(String ok, String fail, List<String> fork) {
            this.ok = ok;
            this.fail = fail;
            this.fork = fork;
        }

        public boolean isJoin() {
            return fail == null && ok != null;
        }

        public boolean isFork() {
            return fork != null;
        }
    }
}
