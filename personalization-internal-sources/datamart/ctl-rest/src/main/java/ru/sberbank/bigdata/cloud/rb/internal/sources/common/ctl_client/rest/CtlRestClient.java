package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.rest;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.Constants.CTL_API_V5;


public class CtlRestClient {
    public static final String DELETED_FIELD_NAME = "deleted";
    private static final Logger log = LoggerFactory.getLogger(CtlRestClient.class);
    private URL ctlRestBaseUrl;

    public CtlRestClient(String ctlUrl) throws MalformedURLException {
        URL baseUrl = new URL(ctlUrl);
        this.ctlRestBaseUrl = new URL(baseUrl, CTL_API_V5 + "/");
    }

    private static void putToJSONObjectIfNotNull(JSONObject jsonObject, String key, Object value) throws JSONException {
        if (value != null) {
            jsonObject.put(key, value);
        }
    }

    private static void putArrayToJSONObjectIfNotNull(JSONObject jsonObject, String key, List list) throws JSONException {
        if (list != null) {
            JSONArray jsonArray = new JSONArray();
            putToJSONArray(jsonArray, list);
            jsonObject.put(key, jsonArray);
        }

    }

    private static void putToJSONArrayIfNotNull(JSONArray jsonArray, List list) {
        if (list != null) {
            Iterator var2 = list.iterator();

            while (var2.hasNext()) {
                Object e = var2.next();
                jsonArray.put(e);
            }
        }

    }

    private static void putToJSONArray(JSONArray jsonArray, List list) {
        Iterator var2 = list.iterator();

        while (var2.hasNext()) {
            Object e = var2.next();
            jsonArray.put(e);
        }

    }

    public RequestResult getCtlInfo() throws JSONException, IOException {
        String relativeUrl = String.format("info");
        return this.sendRequest(relativeUrl, RequestMethod.GET);
    }

    public RequestResult getWfByName(String workflowName) throws JSONException, IOException {
        String relativeUrl = String.format("wf/name/%s", workflowName);
        return this.sendRequest(relativeUrl, RequestMethod.GET);
    }

    public RequestResult getLoading(int loadingId) throws JSONException, IOException {
        String relativeUrl = String.format("loading/%s", loadingId);
        return this.sendRequest(relativeUrl, RequestMethod.GET);
    }

    public RequestResult getProfile(String profile) throws JSONException, IOException {
        String relativeUrl = String.format("profile/name/%s", profile);
        return this.sendRequest(relativeUrl, RequestMethod.GET);
    }

    public String getWorkflowNameByLoadingId(int loadingId) throws JSONException, IOException {
        RequestResult loadingRequestResult = this.getLoading(loadingId);
        JSONObject workflowJson = (new JSONObject(loadingRequestResult.getResposeText())).getJSONObject("workflow");
        return workflowJson.getString("name");
    }

    public int getWorkflowIdByName(String workflowName) throws JSONException, IOException {
        RequestResult workflowRequestResult = this.getWfByName(workflowName);
        JSONArray workflowsArray = new JSONArray(workflowRequestResult.getResposeText());
        int workflowId = -1;

        for (int i = 0; i < workflowsArray.length(); ++i) {
            JSONObject workflow = workflowsArray.getJSONObject(i);
            boolean isDeleted = workflow.getBoolean("deleted");
            if (!isDeleted) {
                workflowId = workflow.getInt("id");
                break;
            }
        }

        return workflowId;
    }

    public RequestResult getStatValByEntityAndLoadingId(int entityId, int statId, int loadingId) throws JSONException, IOException {
        String relativeUrl = String.format("statval/%s/%s/%s", entityId, statId, loadingId);
        return this.sendRequest(relativeUrl, RequestMethod.GET);
    }

    public RequestResult getAllStatisticsForEntityWithProfile(int entityId, int statId, String profile) throws JSONException, IOException {
        String relativeUrl = String.format("statval/%s/%s/%s", entityId, statId, profile);
        return this.sendRequest(relativeUrl, RequestMethod.GET);
    }

    public RequestResult getAllStatistics(int entityId, int limit, int offset) throws JSONException, IOException {
        String relativeUrl = String.format("statval/all/entity/%s/%s/%s", entityId, limit, offset);
        return this.sendRequest(relativeUrl, RequestMethod.GET);
    }

    public RequestResult runWf(int wfId) throws JSONException, IOException {
        String relativeUrl = String.format("wf/sched/%s", wfId);
        JSONObject requestBody = new JSONObject();
        return this.sendRequest(relativeUrl, RequestMethod.POST, requestBody.toString());
    }

    public RequestResult stopWf(int loadingId) throws JSONException, IOException {
        String relativeUrl = String.format("loading/%s", loadingId);
        return this.sendRequest(relativeUrl, RequestMethod.PUT);
    }

    public RequestResult getEntityById(int entityId) throws JSONException, IOException {
        String relativeUrl = String.format("entity/%s", entityId);
        return this.sendRequest(relativeUrl, RequestMethod.GET);
    }

    public String getStatValValueByEntityAndLoadingId(int entityId, int statId, int loadingId) throws JSONException, IOException {
        RequestResult statvalRequestResult = this.getStatValByEntityAndLoadingId(entityId, statId, loadingId);
        JSONObject statValJson = (new JSONArray(statvalRequestResult.getResposeText())).getJSONObject(0);
        return statValJson.getString("value");
    }

    public RequestResult setLoadingStatus(int loadingId, CtlLoadingStatus status) throws JSONException, IOException {
        return this.setLoadingStatusWithLog(loadingId, status, (String) null);
    }

    public RequestResult setLoadingStatusWithLog(int loadingId, CtlLoadingStatus status, String logMessage) throws JSONException, IOException {
        String relativeUrl = "loading/status";
        JSONObject requestBody = new JSONObject();
        requestBody.put("loading_id", loadingId);
        requestBody.put("status", status);
        if (logMessage != null) {
            requestBody.put("log", logMessage);
        }

        return this.sendRequest(relativeUrl, RequestMethod.PUT, requestBody.toString());
    }

    public RequestResult abortLoading(int loadingId) throws JSONException, IOException {
        String relativeUrl = String.format("loading/%s", loadingId);
        return this.sendRequest(relativeUrl, RequestMethod.DELETE);
    }

    public int getLastSuccessfulCompletedLoadingIdByWorkflowId(int workflowId) throws JSONException, IOException {
        RequestResult loadingsListRequestResult = this.getLastSuccessfulCompletedLoadingByWorkflowId(workflowId);

        Integer loadingId;
        try {
            loadingId = this.getLoadingIdFromLoadingListRequestResult(loadingsListRequestResult, 0);
        } catch (JSONException var5) {
            loadingId = -1;
        }

        return loadingId;
    }

    public RequestResult getLastSuccessfulCompletedLoadingByWorkflowId(int workflowId) throws JSONException, IOException {
        int numberOfLoadingsToGet = 1;
        List<CtlLoadingState> loadingStates = Collections.singletonList(CtlLoadingState.COMPLETED);
        List<CtlLoadingStatus> loadingStatuses = Collections.singletonList(CtlLoadingStatus.SUCCESS);
        LoadingFilterBuilder filterBuilder = new LoadingFilterBuilder();
        filterBuilder.setWorkflowId(workflowId);
        filterBuilder.setLoadingStates(loadingStates);
        filterBuilder.setLoadingStatuses(loadingStatuses);
        filterBuilder.setNumberOfLoadingsToGet(Integer.valueOf(numberOfLoadingsToGet));
        LoadingFilter loadingFilter = filterBuilder.createLoadingFilter();
        return this.getLoadingsByFilter(loadingFilter);
    }

    public int getPenultimateLoadingIdByWorkflowId(int workflowId) throws JSONException, IOException {
        RequestResult loadingsListRequestResult = this.getPenultimateLoadingByWorkflowId(workflowId);

        Integer loadingId;
        try {
            loadingId = this.getLoadingIdFromLoadingListRequestResult(loadingsListRequestResult, 1);
        } catch (JSONException var5) {
            loadingId = -1;
        }

        return loadingId;
    }

    public RequestResult getPenultimateLoadingByWorkflowId(int workflowId) throws JSONException, IOException {
        int numberOfLoadingsToGet = 2;
        LoadingFilterBuilder filterBuilder = new LoadingFilterBuilder();
        filterBuilder.setWorkflowId(workflowId);
        filterBuilder.setNumberOfLoadingsToGet(Integer.valueOf(numberOfLoadingsToGet));
        LoadingFilter loadingFilter = filterBuilder.createLoadingFilter();
        return this.getLoadingsByFilter(loadingFilter);
    }

    public RequestResult getLoadingsByFilter(LoadingFilter filter) throws JSONException, IOException {
        String relativeUrl = "loading/filtered";
        JSONObject requestBody = new JSONObject();
        putToJSONObjectIfNotNull(requestBody, "wf_id", filter.getWorkflowId());
        putToJSONObjectIfNotNull(requestBody, "xid", filter.getXid());
        putArrayToJSONObjectIfNotNull(requestBody, "alive", filter.getLoadingStates());
        putArrayToJSONObjectIfNotNull(requestBody, "status", filter.getLoadingStatuses());
        putToJSONObjectIfNotNull(requestBody, "start", filter.getStartTime());
        putToJSONObjectIfNotNull(requestBody, "end", filter.getEndTime());
        putToJSONObjectIfNotNull(requestBody, "limit", filter.getNumberOfLoadingsToGet());
        putToJSONObjectIfNotNull(requestBody, "offset", filter.getOffset());
        return this.sendRequest(relativeUrl, RequestMethod.POST, requestBody.toString());
    }

    private int getLoadingIdFromLoadingListRequestResult(RequestResult loadingsListRequestResult, int index) throws JSONException {
        JSONArray loadingsListJson;
        try {
            loadingsListJson = new JSONArray(loadingsListRequestResult.getResposeText());
        } catch (JSONException var5) {
            log.error("Request returned: " + loadingsListRequestResult.getResposeText());
            throw var5;
        }

        return loadingsListJson.getJSONObject(index).getInt("id");
    }

    public RequestResult publishStatistic(int loadingId, int entityId, int statId, String statValue) throws JSONException, IOException {
        String relativeUrl = "statval/m";
        JSONObject requestBody = new JSONObject();
        requestBody.put("loading_id", loadingId);
        requestBody.put("entity_id", entityId);
        requestBody.put("stat_id", statId);
        JSONArray statValues = new JSONArray();
        statValues.put(statValue);
        requestBody.put("avalue", statValues);
        System.out.println("body is: " + requestBody.toString());
        return this.sendRequest(relativeUrl, RequestMethod.POST, requestBody.toString());
    }

    public RequestResult createWf(String name, String type, String engine, boolean scheduled, String category, String profile) throws JSONException, IOException {
        String relativeUrl = "wf";
        JSONObject requestBody = new JSONObject();
        requestBody.put("name", name);
        requestBody.put("type", type);
        requestBody.put("engine", engine);
        requestBody.put("scheduled", scheduled);
        requestBody.put("category", category);
        requestBody.put("profile", profile);
        return this.sendRequest(relativeUrl, RequestMethod.POST, requestBody.toString());
    }

    private RequestResult sendRequest(String relativeUrl, RequestMethod requestMethod) throws JSONException, IOException {
        return this.sendRequest(relativeUrl, requestMethod, (String) null);
    }

    private RequestResult sendRequest(String relativeUrl, RequestMethod requestMethod, String requestBody) throws JSONException, IOException {
        URL url = new URL(this.ctlRestBaseUrl, relativeUrl);
        String krb5conf = System.getProperty("java.security.krb5.conf", "/etc/krb5.conf");
        System.setProperty("java.security.krb5.conf", krb5conf);
        System.setProperty("java.security.auth.useSubjectCredsOnly", "false");
        System.setProperty("java.security.auth.login.config", "jaas.conf");
        System.setProperty("sun.security.krb5.debug","true");

        String requestInfo = String.format("========== Sending request %s to %s.%n", requestMethod.name(), url.toString());
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod(requestMethod.name());
        if (requestBody != null) {
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);
            OutputStreamWriter wr = new OutputStreamWriter(conn.getOutputStream());
            wr.write(requestBody);
            wr.close();
        }

        int status = conn.getResponseCode();
        String inputLine;
        if (log.isInfoEnabled()) {
            inputLine = String.format("========== Request %s to %s returned %s.%n", requestMethod.name(), url.toString(), status);
            log.info(inputLine);
        }

        StringBuilder content = new StringBuilder();
        BufferedReader err;
        if (status >= 200 && status < 300) {
            err = new BufferedReader(new InputStreamReader(conn.getInputStream()));

            while ((inputLine = err.readLine()) != null) {
                content.append(inputLine);
            }

            err.close();
            conn.disconnect();
            return new RequestResult(status, content.toString());
        } else {
            err = new BufferedReader(new InputStreamReader(conn.getErrorStream()));

            while ((inputLine = err.readLine()) != null) {
                content.append(inputLine);
            }

            err.close();
            conn.disconnect();
            String errorMsg = String.format("Request %s to %s returned %s.%nError message: %s", requestMethod.name(), url.toString(), status, content.toString());
            throw new RuntimeException(errorMsg);
        }
    }

    public String getLastStatValByEntityAndLStatId(int entityId, int statId) throws JSONException, IOException {
        String relativeUrl = String.format("statval/%s/%s", entityId, statId);
        final RequestResult statvalRequestResult = this.sendRequest(relativeUrl, RequestMethod.GET);
        JSONArray jsonArray = new JSONArray(statvalRequestResult.getResposeText());
        List<JSONObject> jsonValues = new ArrayList();

        for (int i = 0; i < jsonArray.length(); ++i) {
            jsonValues.add(jsonArray.getJSONObject(i));
        }

        jsonValues.sort(new Comparator<JSONObject>() {
            private static final String KEY_NAME = "loading_id";

            public int compare(JSONObject o1, JSONObject o2) {
                Integer val1;
                Integer val2;
                try {
                    val1 = (Integer) o1.get("loading_id");
                    val2 = (Integer) o2.get("loading_id");
                } catch (JSONException var6) {
                    CtlRestClient.log.error("Request returned: " + statvalRequestResult.getResposeText());
                    throw new RuntimeException(var6);
                }

                return -val1.compareTo(val2);
            }
        });
        return ((JSONObject) jsonValues.get(0)).getString("value");
    }



    private static enum RequestMethod {
        GET,
        POST,
        DELETE,
        PUT;

        private RequestMethod() {
        }
    }
}

