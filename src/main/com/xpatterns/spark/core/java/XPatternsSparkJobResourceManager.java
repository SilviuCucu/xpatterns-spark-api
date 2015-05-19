package com.xpatterns.spark.core.java;

import com.typesafe.config.Config;
import com.xpatterns.spark.core.java.instrumentation.logger.XPatternsLogger;
import org.apache.commons.io.FilenameUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.IOException;


/**
 * Created by silviuc on 07.10.2014.
 */
public class XPatternsSparkJobResourceManager {
    public static byte[] getResourceByName(String relativePath, String resourceName, Config configuration, XPatternsLogger logger) throws Exception {
        String tcomponentUrl = configuration.getString("tcomponentUrl");
        String jobId = configuration.getString("spark.xpatterns.jobId");
        String stageId = configuration.getString("spark.xpatterns.stageId");

        logger.info("[XPatternsSparkJobResourceManager] Resource relative path \"" + relativePath + "\", resource name \"" + resourceName +
                "\", jobId " + jobId + ", stageId " + stageId + ", TComponent endpoint " + tcomponentUrl);

        try {
            HttpClient httpclient = HttpClientBuilder.create().build();

            String uri;
            if (relativePath == null || relativePath.isEmpty()) {
                uri = tcomponentUrl + String.format("/jobs/%s/stages/%s/resources?name=%s", jobId, stageId, resourceName);
            } else {
                uri = tcomponentUrl + String.format("/jobs/%s/stages/%s/resources?relativePath=%s&name=%s", jobId, stageId, relativePath, resourceName);
            }
            HttpGet httpGet = new HttpGet(uri);
            HttpResponse response = httpclient.execute(httpGet);
            int statusCode = response.getStatusLine().getStatusCode();
            HttpEntity responseEntity = response.getEntity();

            if (statusCode == 200 || statusCode == 202) {
                byte[] resourceContent = EntityUtils.toByteArray(responseEntity);
                logger.info("[XPatternsSparkJobResourceManager] Resource content length: " + resourceContent.length);
                return resourceContent;
            } else {
                throw new RuntimeException("Http request for resource by name FAILED! Status code " + response.getStatusLine().getStatusCode() +
                        ", URI " + uri + ", response body:\n" + EntityUtils.toString(responseEntity));
            }
        } catch (ClientProtocolException e) {
            logger.error("[XPatternsSparkJobResourceManager] ClientProtocolException while retrieving resource via HTTP: " + e.getMessage());
            throw e;
        } catch (IOException e) {
            logger.error("[XPatternsSparkJobResourceManager] IOException while reading resource content: " + e.getMessage());
            throw e;
        }
    }

    public static byte[] getResourceByName(String resourceName, Config configuration, XPatternsLogger logger) throws Exception {
        return getResourceByName("", resourceName, configuration, logger);
    }

    public static byte[] getResourceByFilePath(String resourceRelativePath, Config configuration, XPatternsLogger logger) throws Exception {
        String folderPath = FilenameUtils.getPathNoEndSeparator(resourceRelativePath);
        String resourceName = FilenameUtils.getName(resourceRelativePath);

        return getResourceByName(folderPath, resourceName, configuration, logger);
    }
}
