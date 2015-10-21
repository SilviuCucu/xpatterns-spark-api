package com.xpatterns.spark.core.java;

import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.FileEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Map;


/**
 * Created by radum on 10.04.2014.
 */

public abstract class SparkJobRestClient {
    public static final String VERSION = "3.0";
    public static final String SPARK_UI_PORT = "sparkUiPort";
    private String serverURI;
    public final SimpleDateFormat CUSTOM_DATE = new SimpleDateFormat("\"yyyy-MM-dd HH:mm:ss\"");


    public SparkJobRestClient(String uri) {
        this.serverURI = uri;
    }

    public String getServerURI() {
        return serverURI;
    }

    public abstract void writeInfo(String info);

    public abstract void writeError(String error);

    protected abstract String getXPatternsSjsBridgeClassPath();


    public String launchJob(String input, String contextName, Boolean sync) throws Exception {

        if (checkContextExist(contextName)) {
            writeInfo("Launching Job on existing context <" + contextName + ">");
        } else {
            throw new RuntimeException("Job Context <" + contextName + "> does not exist!");
        }
        HttpClient httpclient = HttpClients.createDefault();


        String uri = String.format("%s/jobs?runningClass=%s&contextName=%s", getServerURI(), getXPatternsSjsBridgeClassPath(), contextName);
        writeInfo("Launching Job URI: " + uri);

        HttpPost httpPost = new HttpPost(uri);
        httpPost.setEntity(new StringEntity(input));

        HttpResponse response = httpclient.execute(httpPost);


        validateResponseURI(response, uri);

        String requestResult = EntityUtils.toString(response.getEntity());
        Map<String, Object> values = new Gson().fromJson(requestResult, Map.class);


        if (requestResult.contains("ERROR")) {
            writeError("RequestResult: " + requestResult);
            throw new RuntimeException(requestResult);
        }


        writeInfo("Job [" + values.get("jobId") + "] launched successfully, starting execution");

        return (String) values.get("jobId");
    }

    private boolean checkHttpResponse(HttpResponse response) throws Exception {

        HttpEntity entity = response.getEntity();
        int statusCode = response.getStatusLine().getStatusCode();

        String logPrefix = "SparkJobRestClient->checkHttpResponse";

        if (entity == null) {
            writeInfo("SparkJobRestClient->checkHttpResponse->Http entity response is null!");
        }

        if (200 != statusCode && 202 != statusCode) {
            throw new RuntimeException(logPrefix + "Http Response [Status: FAILED]!!!\nStatusCode:" + statusCode + "\nErrorMessage: " + getHttpResponseEntityAsString(response));
        }

        return true;
    }

    // ATTENTION if the stream is consumed twice U should expect java.io.IOException: Attempted read from closed stream
    private String getHttpResponseEntityAsString(HttpResponse response) {
        String responseString = "";

        try {
            responseString = EntityUtils.toString(response.getEntity(), "UTF-8");
        } catch (IOException e) {
            throw new RuntimeException("HttpResponse->getHttpResponseEntityAsString was called twice!!! Avoid that!: " + e);
        }

        //writeInfo("HttpResponse->Entity content: " + responseString);
        return responseString;
    }


    private boolean validateResponseURI(HttpResponse response, String uri) throws Exception {
        int statusCode = response.getStatusLine().getStatusCode();
        final String message = "SparkJobRestClient->HttpResponse->checkHttpResponse->statusCode: " + statusCode;
        if (200 != statusCode && 202 != statusCode) {
            throw new RuntimeException("Response FAILED!!!" + message + "***URI***" + uri + "---statusResponse---" + response.getStatusLine().getReasonPhrase());
        }

        return true;
    }


    public void deleteContext(String contextName) throws Exception {
        // check if context exists before deletion
        if (checkContextExist(contextName)) {
            HttpClient httpclient = HttpClients.createDefault();
            String uri = String.format("%s/contexts/%s", getServerURI(), contextName);

            HttpDelete httpDelete = new HttpDelete(uri);

            int retryTimes = 1;
            boolean validateDeleteResponse = false;
            while (retryTimes <= 5 && validateDeleteResponse == false) {
                try {
                    HttpResponse response = httpclient.execute(httpDelete);
                    checkHttpResponse(response);
                    writeInfo(EntityUtils.toString(response.getEntity()));
                    validateDeleteResponse = true;
                    writeInfo("Context " + contextName + " deleted!");
                } catch (Exception ex) {
                    ex.printStackTrace();
                    writeError(ex.getMessage() + "\n Retry count: " + retryTimes);
                    retryTimes++;
                }
            }
        } else {
            writeInfo("Context <" + contextName + "> doesn't exist!");
        }
        Thread.sleep(5000);

    }

    public boolean checkContextExist(String contextName) throws Exception {

        HttpClient httpclient = HttpClients.createDefault();
        String uri = String.format("%s/contexts/%s", getServerURI(), contextName);

        HttpGet httpGet = new HttpGet(uri);

        HttpResponse response = httpclient.execute(httpGet);

        boolean result = 400 == response.getStatusLine().getStatusCode() ? false : true;
        return result;
    }

    public void createApp(String appName, String resourcePath) throws Exception {
        HttpClient httpclient = HttpClients.createDefault();
        String uri = String.format("%s/jars/%s", getServerURI(), appName);

        HttpPost httpPost = new HttpPost(uri);
        FileEntity fileEntity = new FileEntity(new File(resourcePath), "binary/octet-stream");
        httpPost.setEntity(fileEntity);

        HttpResponse response = httpclient.execute(httpPost);
        checkHttpResponse(response);
        writeInfo(EntityUtils.toString(response.getEntity()));
    }

    public String getJobStatus(String jobId, String contextName) throws Exception {
        HttpClient httpclient = HttpClients.createDefault();
        String uri = String.format("%s/jobs/%s?contextName=%s", getServerURI(), jobId, contextName);
        HttpGet httpGet = new HttpGet(uri);

        HttpResponse response = httpclient.execute(httpGet);
        checkHttpResponse(response);

        String requestResponse = EntityUtils.toString(response.getEntity());
        Map<String, Object> values = new Gson().fromJson(requestResponse, Map.class);

        return values == null ? "Status is not retrieved!!!" : (String) values.get("status");

    }

    public Integer createContext(String contextName, HashSet<String> parameters, int postCreationWaitTime) throws Exception {
        writeInfo("Context <" + contextName + "> will be created");
        boolean resultResponse = false;
        HttpResponse response = null;

        HttpClient httpclient = HttpClients.createDefault();
        String uri = String.format("%s/contexts/%s",
                getServerURI(), contextName);

        writeInfo("Context URI: " + uri);

        String finalParameters = "";
        for (String entry : parameters) {
            finalParameters += entry + "\n";
        }
        HttpPost httpPost = new HttpPost(uri);
        httpPost.setEntity(new StringEntity(finalParameters));
        try {
            response = httpclient.execute(httpPost);
            resultResponse = checkHttpResponse(response);

        } catch (RuntimeException e) {
            //this comes only from validationResponse line!!!
            throw new RuntimeException("Context <" + contextName + "> was NOT created! Caused by..." + e.getMessage());
        } catch (Exception e) {
            throw new RuntimeException("Context <" + contextName + "> was NOT created! Caused by..." + getHttpResponseEntityAsString(response));
        }

        if (resultResponse) {

            String responseAsString = getHttpResponseEntityAsString(response);

            Map<String, String> values = new Gson().fromJson(responseAsString, Map.class);

            String sparkUiPort = values.get(SPARK_UI_PORT);
            if (values != null && sparkUiPort != null)
                return Integer.valueOf(sparkUiPort);
        } else {
            throw new RuntimeException("Context creation failed!");
        }

        return -1;
    }

    //Hadoop Configuration
    public void uploadFileToHDFS(String path, byte[] bytes, String hdfsHost, String hdfsUser) throws IOException {
        Configuration conf = getHadoopConf(hdfsHost, hdfsUser);
        FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream output = fs.create(new Path(path));
        output.write(bytes);
        output.close();
    }

    public Configuration getHadoopConf(String hdfsHost, String hdfsUser) {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://" + hdfsHost + ":8020");

        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("hadoop.job.ugi", hdfsUser);

        return conf;
    }

}
