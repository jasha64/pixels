package io.pixelsdb.pixels.worker.common;

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
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsReaderImpl;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.DictionaryColumnVector;
import io.pixelsdb.pixels.core.vector.LongColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Test;

import java.nio.ByteBuffer;

import static io.netty.handler.codec.http.HttpHeaderNames.*;
import static io.netty.handler.codec.http.HttpHeaderValues.CLOSE;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.pixelsdb.pixels.storage.s3.Minio.ConfigMinio;

public class TestHttpServerClient {

    @Test
    public void testServer() throws Exception {
        ConfigMinio("dummy-region", "http://hp012.utah.cloudlab.us:9000", "5hBbowVNnecVfpOXikrw", "oRB8qTbvsYcmKotnFhECjlJulCOzJSoVmbo72IyU");
        Storage minio = StorageFactory.Instance().getStorage(Storage.Scheme.minio);
        System.out.println(minio.listPaths("pixels-tpch/").size() + " .pxl files on Minio");
//        List<String> files = minio.listPaths("pixels-tpch/");
//        for (String file : files)
//        {
//            System.out.println(file);
//        }

        PixelsReader reader = PixelsReaderImpl.newBuilder()
                .setStorage(minio)
                .setPath("pixels-tpch/nation/v-0-ordered/20230802135121_105.pxl")
                .setEnableCache(false)
                .setPixelsFooterCache(new PixelsFooterCache())
                .build();
        PixelsReaderOption option = new PixelsReaderOption();
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        option.enableEncodedColumnVector(true);
        String[] colNames = new String[]{"n_nationkey", "n_name", "n_regionkey"};
        option.includeCols(colNames);
//        option.rgRange(0, 1);
//        option.transId(1);
        PixelsRecordReader recordReader = reader.read(option);
        VectorizedRowBatch rowBatch = recordReader.readBatch(1000);
        System.out.println(rowBatch.size + " rows read from tpch nation.pxl");
//        DictionaryColumnVector vector = (DictionaryColumnVector) rowBatch.cols[1];
//        System.out.println(vector.toString(0));

        io.pixelsdb.pixels.common.utils.HttpServer h;
        h = new io.pixelsdb.pixels.common.utils.HttpServer(new io.pixelsdb.pixels.common.utils.HttpServerHandler() {
            @Override
            public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
                if (msg instanceof HttpRequest) {
                    HttpRequest req = (HttpRequest) msg;

                    CommonProto.Metadata.Builder message = CommonProto.Metadata.newBuilder();
//                            .addTypes(typeBuild.setName("n_nationkey").setKind(CommonProto.Type.Kind.LONG).build())
//                            .addTypes(typeBuild.setName("n_name").setKind(CommonProto.Type.Kind.DICT).build())
//                            .addTypes(typeBuild.setName("n_regionkey").setKind(CommonProto.Type.Kind.LONG).build())
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

                    byte[] magicBytes = Constants.MAGIC.getBytes();  // 6 bytes
                    byte[] messageBytes = message.build().toByteArray();  // 4 bytes
                    int messageLength = messageBytes.length;
                    byte[] messageLengthBytes = ByteBuffer.allocate(4).putInt(messageLength).array();
                    int paddingLength = (magicBytes.length + messageLengthBytes.length + messageLength) % 8;
                    byte[] paddingBytes = new byte[paddingLength];
                    // todo message body

                    byte[] combinedMessage =
                            ArrayUtils.addAll(
                            ArrayUtils.addAll(
                            ArrayUtils.addAll(
                            magicBytes, messageLengthBytes), messageBytes), paddingBytes);
//                    System.out.println(combinedMessage.length);
                    FullHttpResponse response = new DefaultFullHttpResponse(req.protocolVersion(), OK,
                            Unpooled.wrappedBuffer(combinedMessage));
                    response.headers()
                            .set(CONTENT_TYPE, "application/x-protobuf")
                            .setInt(CONTENT_LENGTH, response.content().readableBytes());

                    // serve only once, so that we pass the test instead of hanging
                    response.headers().set(CONNECTION, CLOSE);
                    ChannelFuture f = ctx.writeAndFlush(response);
//                    f.addListener(ChannelFutureListener.CLOSE);
//                    ctx.close();
                    ctx.writeAndFlush(Unpooled.EMPTY_BUFFER)
                            .addListener(ChannelFutureListener.CLOSE)
                            .addListener(future -> {
                                // Gracefully shutdown the server after the channel is closed
                                ctx.channel().parent().close().addListener(ChannelFutureListener.CLOSE);
                            });
                }
            }
        });
        h.serve();
    }

    @Test
    public void testClient() {

    }
}
