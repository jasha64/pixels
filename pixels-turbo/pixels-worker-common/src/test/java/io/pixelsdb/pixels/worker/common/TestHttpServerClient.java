package io.pixelsdb.pixels.worker.common;

import com.google.common.collect.ImmutableList;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;
import io.pixelsdb.pixels.common.CommonProto;
import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.PhysicalReaderUtil;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.core.PixelsFooterCache;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsReaderImpl;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.DictionaryColumnVector;
import io.pixelsdb.pixels.core.vector.LongColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.cert.CertificateException;
import java.util.List;

import static io.netty.handler.codec.http.HttpHeaderNames.*;
import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaderValues.CLOSE;
import static io.netty.handler.codec.http.HttpHeaderValues.KEEP_ALIVE;
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

                    byte[] messageBytes = message.build().toByteArray();
                    ByteBuffer combinedBuffer = ByteBuffer.allocate(messageBytes.length);
                    combinedBuffer.put(messageBytes);
                    FullHttpResponse response = new DefaultFullHttpResponse(req.protocolVersion(), OK,
                            Unpooled.wrappedBuffer(messageBytes));
                    response.headers()
                            .set(CONTENT_TYPE, "application/x-protobuf")
                            .setInt(CONTENT_LENGTH, messageBytes.length);

                    // serve only once, so that we pass the test instead of hanging
                    response.headers().set(CONNECTION, CLOSE);
                    ChannelFuture f = ctx.write(response);
                    f.addListener(ChannelFutureListener.CLOSE);
                }
            }
        });
        h.serve();
    }

    @Test
    public void testClient() {

    }
}
