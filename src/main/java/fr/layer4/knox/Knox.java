package fr.layer4.knox;

import com.jayway.jsonpath.JsonPath;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.Version;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.gateway.shell.BasicResponse;
import org.apache.hadoop.gateway.shell.Hadoop;
import org.apache.hadoop.gateway.shell.hdfs.Hdfs;
import org.apache.hadoop.gateway.shell.yarn.Yarn;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Slf4j
public class Knox {

    public static void main(String... args) throws Exception {

        String url = "https://knox.server:8443/gateway/default";
        String username = "";
        String password = "";
        String appName = "My app";
        String hdpVersion = "2.3.4.0-361";
        String jvmHome = "/usr/jdk64/jdk1.8.0_60/";
        String sparkJarPath = "hdfs://spark.jar"; // The assembly spark jar
        String appJarPath = "hdfs://app.jar";
        String appPropertiesPath = "hdfs://app.properties";
        String lzoJarPath = "";
        String className = "com.myapp.MyClass";
        int applicationMasterMemory = 8192;
        int applicationMasterCores = 1;
        // Optional
        String keytab = "";
        String principal = "";
        String userCredentialPath = ""; // Must end with a '/'

        // Prepare session
        Hadoop hadoop = Hadoop.login(url, username, password);

        // Find spark.jar
        Map<String, Object> sparkJarStatus = findStatus(hadoop, sparkJarPath);

        // Find app.jar
        Map<String, Object> appJarStatus = findStatus(hadoop, appJarPath);

        // Find app.properties
        Map<String, Object> appPropertiesStatus = findStatus(hadoop, appPropertiesPath);

        // Create a new app
        String appId = createNewApp(hadoop);

        // Prepare a model for the template
        String kerberosOptions = "";
        if (StringUtils.isNoneBlank(keytab) && StringUtils.isNoneBlank(principal) && StringUtils.isNoneBlank(userCredentialPath)) {
            String credentialsFile = userCredentialPath + "credentials_" + UUID.randomUUID().toString();
            kerberosOptions = " -Dspark.yarn.keytab= " + keytab +
                    " -Dspark.yarn.principal= " + principal +
                    " -Dspark.yarn.credentials.file= " + credentialsFile +
                    " -Dspark.history.kerberos.keytab= " + keytab +
                    " -Dspark.history.kerberos.principal= " + principal +
                    " -Dspark.history.kerberos.enabled=true";
        }

        Map<String, Object> data = new HashMap<>();
        data.put("appId", appId);
        data.put("appName", appName);
        data.put("hdpVersion", hdpVersion);
        data.put("jvmHome", jvmHome);
        data.put("sparkJarStatus", sparkJarStatus);
        data.put("sparkJarPath", sparkJarStatus);
        data.put("appJarStatus", appJarStatus);
        data.put("appJarPath", appJarPath);
        data.put("appPropertiesStatus", appPropertiesStatus);
        data.put("appPropertiesPath", appPropertiesPath);
        data.put("lzoJarPath", lzoJarPath);
        data.put("className", className);
        data.put("applicationMasterMemory", applicationMasterMemory);
        data.put("applicationMasterCores", applicationMasterCores);
        data.put("kerberosOptions", kerberosOptions);

        // Generate JSON
        String json = renderTemplate(data);

        // Submit app
        submitApp(hadoop, json);

        // Track the app
        String state = null;
        while (!"RUNNING".equals(state) && !"ACCEPTED".equals(state)) {
            state = trackApp(hadoop, appId);
            log.info("Status: {}", state);
        }
    }

    private static String renderTemplate(Map<String, Object> data) throws IOException, TemplateException {
        StringWriter writer = new StringWriter();
        Configuration cfg = new Configuration(new Version(2, 3, 23));
        Template template = cfg.getTemplate(Knox.class.getClassLoader().getResource("submit.json.template").getPath());
        template.process(data, writer);
        return writer.toString();
    }

    private static Map<String, Object> findStatus(Hadoop hadoop, String path) throws IOException {
        // Just a trick here, you use LISTSTATUS because GETFILESTATUS is not yet implemented in Knox Java client
        BasicResponse response = null;
        try {
            response = Hdfs.ls(hadoop).dir(path).now();
            return JsonPath.read(response.getString(), "$.FileStatuses.FileStatus[0]");
        } finally {
            close("findStatus", response);
        }
    }

    private static void submitApp(Hadoop hadoop, String jsonBody) {
        log.debug("Submitting Spark Job ...");
        BasicResponse response = null;
        try {
            response = Yarn.submitApp(hadoop).text(jsonBody).now();
        } finally {
            close("submitApp", response);
        }
    }

    private static String createNewApp(Hadoop hadoop) throws IOException {
        log.debug("Creating new application ...");
        BasicResponse response = null;
        try {
            response = Yarn.newApp(hadoop).now();
            return JsonPath.read(response.getString(), "$.application-id");
        } finally {
            close("createNewApp", response);
        }
    }

    private static String trackApp(Hadoop hadoop, String appId) throws IOException {
        log.debug("Tracking the app ...");
        BasicResponse response = null;
        try {
            response = Yarn.appState(hadoop).appId(appId).now();
            return JsonPath.read(response.getString(), "$.state");
        } finally {
            close("trackApp", response);
        }
    }

    private static void close(String step, BasicResponse response) {
        if (response != null) {
            log.debug("{} - status: {}", step, response.getStatusCode());
            response.close();
        }
    }
}
