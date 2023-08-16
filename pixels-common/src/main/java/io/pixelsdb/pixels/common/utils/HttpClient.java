package io.pixelsdb.pixels.common.utils;
import io.pixelsdb.pixels.common.CommonProto;
import org.asynchttpclient.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

public class HttpClient {
    public static int calculateCeiling(int value, int multiple) {
        if (value <= 0 || multiple <= 0) {
            throw new IllegalArgumentException("Both value and multiple must be positive.");
        }

        int remainder = value % multiple;
        if (remainder == 0) {
            // No need to adjust, value is already a multiple of multiple
            return value;
        }

        int difference = multiple - remainder;
        int ceiling = value + difference;
        return ceiling;
    }

    public static void main(String[] args) throws IOException {

        try (AsyncHttpClient httpClient = Dsl.asyncHttpClient()) {
            String serverIpAddress = "127.0.0.1";
            int serverPort = 8080;
            Request request = httpClient.prepareGet("http://" + serverIpAddress + ":" + serverPort + "/")
                    .addFormParam("param1", "value1")
                    .build();

            Response response = httpClient.executeRequest(request).get();
            System.out.println("HTTP response status code: " + response.getStatusCode());
            if (Objects.equals(response.getContentType(), "application/x-protobuf")) {
                int messageLength = ByteBuffer.wrap(response.getResponseBodyAsBytes()).getInt(6);
                System.out.println("Parsed objLength: " + messageLength);

                CommonProto.Metadata metadata = CommonProto.Metadata.parseFrom(Arrays.copyOfRange(response.getResponseBodyAsBytes(), 10, 10 + messageLength));
                System.out.println("Parsed object: " + metadata);

                int messageBodyOffset = calculateCeiling(10 + messageLength, 8);
                System.out.println("Parsed messageBodyOffset: " + messageBodyOffset);

                byte[] messageBody = Arrays.copyOfRange(response.getResponseBodyAsBytes(), messageBodyOffset, messageBodyOffset + 205); // messageBodyOffset + metadata.getTypesCount() * 8);
                System.out.println("Parsed messageBody: " + messageBody);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
