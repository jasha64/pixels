package io.pixelsdb.pixels.worker.common;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.pixelsdb.pixels.common.CommonProto;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.core.PixelsFooterCache;
import io.pixelsdb.pixels.core.PixelsReaderImpl;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.DictionaryColumnVector;
import io.pixelsdb.pixels.core.vector.LongColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.common.utils.HttpServer;
import io.pixelsdb.pixels.common.utils.HttpServerHandler;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;
import org.asynchttpclient.*;

import java.nio.ByteBuffer;

import static io.netty.handler.codec.http.HttpHeaderNames.*;
import static io.netty.handler.codec.http.HttpHeaderValues.CLOSE;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.pixelsdb.pixels.storage.s3.Minio.ConfigMinio;

public class TestHttpServerClient {

    @Test
    public void testServerSimple() throws Exception {
        final ByteBuf buffer = Unpooled.buffer(); // Unpooled.directBuffer();
//        System.out.println(buffer.capacity());

        ConfigMinio("dummy-region", "http://hp114.utah.cloudlab.us:9000", "r6SwdB3efI126soLlz4N", "VZOqv43UL94T8G90td1XbVU0kOG7wdexB8Y6dVdL");
        Storage minio = StorageFactory.Instance().getStorage(Storage.Scheme.minio);
//        System.out.println(minio.listPaths("pixels-tpch/").size() + " .pxl files on Minio");
//        List<String> files = minio.listPaths("pixels-tpch/customer/");

        PixelsReaderImpl.Builder reader = PixelsReaderImpl.newBuilder()
                .setStorage(minio)
                .setPath("pixels-tpch/nation/v-0-ordered/20230814143629_105.pxl")
                .setEnableCache(false)
                .setPixelsFooterCache(new PixelsFooterCache());
//        for (String file : files)
//        {
//            System.out.println(file);
//        }
        PixelsReaderOption option = new PixelsReaderOption();
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        option.enableEncodedColumnVector(true);
        String[] colNames = new String[]{"n_nationkey", "n_name", "n_regionkey"};
        option.includeCols(colNames);
        // option.rgRange(0, 1);
        // option.transId(1);
        PixelsRecordReader recordReader = reader.build().read(option);
        VectorizedRowBatch rowBatch = recordReader.readBatch(1000);
        System.out.println(rowBatch.size + " rows read from tpch nation.pxl");
        // DictionaryColumnVector vector = (DictionaryColumnVector) rowBatch.cols[1];
        // System.out.println(vector.toString(0));

        HttpServer h;
        h = new HttpServer(new HttpServerHandler() {
            @Override
            public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
                if (msg instanceof HttpRequest) {
                    HttpRequest req = (HttpRequest) msg;

                    CommonProto.Metadata.Builder message = CommonProto.Metadata.newBuilder();
                            // .addTypes(typeBuild.setName("n_nationkey").setKind(CommonProto.Type.Kind.LONG).build())
                            // .addTypes(typeBuild.setName("n_name").setKind(CommonProto.Type.Kind.DICT).build())
                            // .addTypes(typeBuild.setName("n_regionkey").setKind(CommonProto.Type.Kind.LONG).build())
                    CommonProto.Type.Builder typeBuild = CommonProto.Type.newBuilder();
                    // 似乎把Pixels文件读到内存里的时候丢弃了column names (PixelsRecordReaderImpl.java: 215)。
                    // 所以这里只能依靠前面的option.includeCols(new String[]{"n_nationkey", "n_name", "n_regionkey"});
                    // 作为列名。
                    for (int i = 0; i < rowBatch.cols.length; i++) {
                        ColumnVector col = rowBatch.cols[i];
                        typeBuild.setName(colNames[i]);
                        if (col instanceof LongColumnVector) {
                            typeBuild.setKind(CommonProto.Type.Kind.LONG).build();
                        } else if (col instanceof DictionaryColumnVector) {
                            typeBuild.setKind(CommonProto.Type.Kind.DICT).build();
                        } else {
                            typeBuild.setKind(CommonProto.Type.Kind.LONG).build();
                        }
                        message.addTypes(typeBuild.build());
                    }

                    LongColumnVector col0 = (LongColumnVector) rowBatch.cols[0];
                    for (long col0val : col0.vector) {
                        buffer.writeLong(col0val);
                    }
                    DictionaryColumnVector col1 = (DictionaryColumnVector) rowBatch.cols[1];
                    buffer.writeBytes(col1.dictArray);
                    LongColumnVector col2 = (LongColumnVector) rowBatch.cols[2];
                    for (long col2val : col2.vector) {
                        buffer.writeLong(col2val);
                    }

                    // System.out.println(col0.dictArray);
                    // System.out.println(col0.dictArray.length);
                    byte[] magicBytes = Constants.MAGIC.getBytes();  // 6 bytes
                    byte[] messageBytes = message.setBodyLength(buffer.array().length).build().toByteArray();  // 4 bytes
                    int messageLength = messageBytes.length;
                    byte[] messageLengthBytes = ByteBuffer.allocate(4).putInt(messageLength).array();
                    int paddingLength = (magicBytes.length + messageLengthBytes.length + messageLength) % 8;
                    byte[] paddingBytes = new byte[paddingLength];

                    FullHttpResponse response = new DefaultFullHttpResponse(req.protocolVersion(), OK,
                            Unpooled.wrappedBuffer(magicBytes, messageLengthBytes, messageBytes, paddingBytes, buffer.array()));
                    response.headers()
                            .set(CONTENT_TYPE, "application/x-protobuf")
                            .setInt(CONTENT_LENGTH, response.content().readableBytes());

                    // serve only once, so that we pass the test instead of hanging
                    response.headers().set(CONNECTION, CLOSE);
                    ChannelFuture f = ctx.writeAndFlush(response);
//                    try {
//                        for (int i = 10; i > 0; i--) {
//                            System.out.printf("Handler thread is still running... %d\n", i);
//                            Thread.sleep(1000);
//                        }
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
                    f.addListener(future -> {
                        if (!future.isSuccess()) {
                            System.out.println("Failed to write response: " + future.cause());
//                            ctx.close(); // Close the channel on error
                        }
                    });
                    f.addListener(ChannelFutureListener.CLOSE);
//                            .addListener(future -> {
//                                // Gracefully shutdown the server after the channel is closed
//                                ctx.channel().parent().close().addListener(ChannelFutureListener.CLOSE);
//                            });
                }
            }
        });
        h.serve();
    }

    void runAsync(Runnable fp, int concurrency) {
        ExecutorService executorService = Executors.newFixedThreadPool(1);

        CompletableFuture<Void>[] httpServerFutures = new CompletableFuture[concurrency];
        for (int i = 0; i < concurrency; i++) {
            httpServerFutures[i] = CompletableFuture.runAsync(() -> {
                try {
                    fp.run();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }, executorService);
            if (i < concurrency - 1) {
                System.out.println("Booted " + (i + 1) + " http clients");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        try {
            for (int i = 10; i > 0; i--) {
                System.out.printf("Main thread is still running... %d\n", i);
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
//        // Perform your other calculations asynchronously
//        CompletableFuture<Void> calculationsFuture = CompletableFuture.runAsync(() -> {
//            // Your calculations here
//            System.out.println("Performing calculations...");
//        }, executorService);

        // Wait for both futures to complete
        CompletableFuture<Void> combinedFuture = CompletableFuture.allOf(httpServerFutures); // , calculationsFuture);
        // Block until both futures are completed
        combinedFuture.join();

        // Shutdown the executor service
        executorService.shutdown();
    }

    @Test
    public void testServerAsync() {
        runAsync(() -> {
            try {
                testServerSimple();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 1);
    }

    static int calculateCeiling(int value, int multiple) {
        // to calculate padding length in HttpClient

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

    @Test
    public void testClientSimple() {
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

                final int rowCount = 25;  // todo metadata.getRowCount();
                ByteBuf messageBodyByteBuf = Unpooled.wrappedBuffer(response.getResponseBodyAsBytes(), messageBodyOffset, metadata.getBodyLength());
                long[] col0 = new long[rowCount];
                for (int i = 0; i < rowCount; i++) {
                    col0[i] = messageBodyByteBuf.readLong();
                }
                System.out.println("Parsed col0: " + Arrays.toString(col0));
//                byte[] col1 = new byte[rowCount];
//
//                long[] col2 = new long[rowCount];
//                for (int i = 0; i < rowCount; i++) {
//                    col2[i] = messageBodyByteBuf.readLong();
//                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testClientAsync() {
        runAsync(() -> {
            try {
                testClientSimple();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 1);
    }


    @Test
    public void testClientConcurrent() {
        testClientSimple(); testClientSimple();

//        runAsync(() -> {
//            try {
//                testClientSimple();
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }, 3);
    }
}
