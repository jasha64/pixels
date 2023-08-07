package io.pixelsdb.pixels.common.utils;
import io.pixelsdb.pixels.common.CommonProto;
import org.asynchttpclient.*;
import com.alibaba.fastjson.JSON;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public class HttpClient {
    public static void main(String[] args) throws IOException {
        AsyncHttpClient httpClient = Dsl.asyncHttpClient();

        String serverIpAddress = "127.0.0.1"; // Replace with the actual IP address of the server
        int serverPort = 8080; // Replace with the actual port number of the server

        try {
            int i = 0;

            String json = JSON.toJSONString(i);
            System.out.println("Sending Object: " + json);

            Request request = httpClient.prepareGet("http://" + serverIpAddress + ":" + serverPort + "/")
                    .addFormParam("json", json)
                    .build();

            Response response = httpClient.executeRequest(request).get();
            System.out.println("HTTP response status code: " + response.getStatusCode());
            if (Objects.equals(response.getContentType(), "application/x-protobuf")) {
                System.out.println(response.getResponseBodyAsBytes());
                CommonProto.Metadata metadata = CommonProto.Metadata.parseFrom(Arrays.copyOfRange(response.getResponseBodyAsBytes(), 11, response.getResponseBodyAsBytes().length));
                System.out.println("Parsed object: " + metadata);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            httpClient.close();
        }
    }
}
