package com.xpatterns.spark.core.java;

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
import java.util.Date;
import java.util.HashSet;


/**
 * Created by radum on 10.04.2014.
 */

public abstract class XPatternsSparkJobClient {

    private String serverURI;
    private String XPATTERNS_SJS_BRIDGE_CLASS_PATH = "com.spark.job.server.scala.XPatternsSparkBridge";


    public abstract void writeInfo(String info);

    public abstract void writeError(String error);


    public XPatternsSparkJobClient(String uri) {
        this.serverURI = uri;
        writeInfo("********Spark Job Server Rest URI:" + uri);
    }


    public String getServerURI() {
        return serverURI;
    }

    public String launchXPatternsSparkJob(String mainClass, String jar, HashSet<String> parameters, String context, Boolean sync) throws Exception {

        StringBuffer input = new StringBuffer("mainClass=" + mainClass + "\n libs=\"" + jar + "\"");
        if (parameters != null) {
            input.append("\n");
            for (String pair : parameters) {
                input.append(pair);
                input.append("\n");
            }
        }

        input.append("xpatterns_submission_date=" + new SimpleDateFormat("\"yyyy-MM-dd HH:mm:ss\"").format(new Date()) + "\n");

        return launchJob(input.toString(), context, sync);

    }

    public String launchJob(String input, String contextName, Boolean sync) throws Exception {

        String result = null;
        if (checkContextExist(contextName)) {

            writeInfo("*** Launching Job on existing " + contextName + "!");
        } else {
            throw new RuntimeException("***Launching Job Context " + contextName + " Does not exist!");
        }
        HttpClient httpclient = HttpClients.createDefault();


        String uri = String.format("%s/job?runningClass=%s&context=%s", getServerURI(), XPATTERNS_SJS_BRIDGE_CLASS_PATH, contextName);
        writeInfo("######################## Launch Job URI:" + uri);

        HttpPost httpPost = new HttpPost(uri);
        httpPost.setEntity(new StringEntity(input));

        HttpResponse response = httpclient.execute(httpPost);

        validateResponseURI(response, uri);

        String requestResult = EntityUtils.toString(response.getEntity());

        if (requestResult.contains("ERROR")) {
            throw new RuntimeException(requestResult + " ***URI***" + uri);
        }

        writeInfo("##################### RequestResult ###################\n" + requestResult);

        writeInfo("\n#########################################################");

        return requestResult;
    }

    private boolean checkHttpResponse(HttpResponse response) throws Exception {

        HttpEntity entity = response.getEntity();
        int statusCode = response.getStatusLine().getStatusCode();

        String logPrefix = "XPatternsSparkJobClient->checkHttpResponse";

        if (entity == null) {
            writeInfo("XPatternsSparkJobClient->checkHttpResponse->Http entity response is null!");
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
        final String message = "XPatternsSparkJobClient->HttpResponse->checkHttpResponse->statusCode: " + statusCode;
        if (200 != statusCode && 202 != statusCode) {
            throw new RuntimeException("Response FAILED!!!" + message + "***URI***" + uri + "---statusResponse---" + response.getStatusLine().getReasonPhrase());
        }

        return true;
    }


    public void deleteContext(String contextName) throws Exception {

        writeInfo("Delete context " + contextName + " if exists!");

        // check if context exists before creation
        if (checkContextExist(contextName)) {
            HttpClient httpclient = HttpClients.createDefault();
            String uri = String.format("%s/context/%s", getServerURI(), contextName);

            HttpDelete httpDelete = new HttpDelete(uri);

            int retryTimes = 1;
            boolean validateDeleteResponse = false;
            while (retryTimes <= 5 && validateDeleteResponse == false) {
                try {
                    HttpResponse response = httpclient.execute(httpDelete);
                    writeInfo("Validate delete context response!");
                    checkHttpResponse(response);
                    writeInfo(EntityUtils.toString(response.getEntity()));
                    validateDeleteResponse = true;
                } catch (Exception ex) {
                    ex.printStackTrace();
                    writeError(ex.getMessage() + "\n Retry count: " + retryTimes);
                    retryTimes++;
                }
            }
        }
        Thread.sleep(5000);

    }

    public boolean checkContextExist(String contextName) throws Exception {


        HttpClient httpclient = HttpClients.createDefault();
        String uri = String.format("%s/context/%s", getServerURI(), contextName);

        HttpGet httpGet = new HttpGet(uri);

        HttpResponse response = httpclient.execute(httpGet);

        checkHttpResponse(response);

        String contextResult = EntityUtils.toString(response.getEntity());
        if (contextResult.contains("Context exists")) {
            return true;
        }
        return false;

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
        String uri = String.format("%s/job/?jobId=%s&contextName=%s", getServerURI(), jobId, contextName);
        HttpGet httpGet = new HttpGet(uri);

        HttpResponse response = httpclient.execute(httpGet);
        checkHttpResponse(response);

        String requestResponse = EntityUtils.toString(response.getEntity());


        return requestResponse;

    }

    public Integer createContext(String contextName, HashSet<String> parameters, int postCreationWaitTime) throws Exception {
        writeInfo("Context : " + contextName + " will be created...");
        boolean resultResponse = false;
        HttpResponse response = null;

        HttpClient httpclient = HttpClients.createDefault();
        String uri = String.format("%s/context/%s",
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
            throw new RuntimeException("1Context " + contextName + " was NOT created! Caused by..." + e.getMessage());
        } catch (Exception e) {
            throw new RuntimeException("2Context " + contextName + " was NOT created! Caused by..." + getHttpResponseEntityAsString(response));
        }

        if (resultResponse) {
            Integer sparkUIPort = Integer.valueOf(getHttpResponseEntityAsString(response));
            writeInfo("Context " + contextName + " was created!");
            return sparkUIPort;
        } else {
            throw new RuntimeException("!!! Context creation failed!");
        }
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
