package io.pixelsdb.pixels.common.utils;
import org.asynchttpclient.*;
import com.alibaba.fastjson.JSON;

import java.io.IOException;

public class HttpClient {
    public static void main(String[] args) throws IOException {
        AsyncHttpClient httpClient = Dsl.asyncHttpClient();

        String serverIpAddress = "192.168.0.x"; // Replace with the actual IP address of the server
        int serverPort = 8080; // Replace with the actual port number of the server

        try {
            int i = 0;

            String json = JSON.toJSONString(i);
            System.out.println("Sending Object: " + json);

            Request request = httpClient.preparePost("http://" + serverIpAddress + ":" + serverPort + "/receive-object")
                    .addFormParam("json", json)
                    .build();

            Response response = httpClient.executeRequest(request).get();
            System.out.println("HTTP response status code: " + response.getStatusCode());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            httpClient.close();
        }
    }
}
