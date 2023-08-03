package io.pixelsdb.pixels.common.utils;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpUtil;
import io.pixelsdb.pixels.common.CommonProto;

import java.nio.ByteBuffer;

import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaderValues.CLOSE;
import static io.netty.handler.codec.http.HttpHeaderValues.KEEP_ALIVE;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

public class HttpServerHandler extends SimpleChannelInboundHandler<HttpObject> {
    private byte[] payload = { 'H', 'e', 'l', 'l', 'o', ' ', 'W', 'o', 'r', 'l', 'd' };

    public HttpServerHandler() {}
    public HttpServerHandler(byte[] a) {payload = a;}

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
        if (msg instanceof HttpRequest) {
            HttpRequest req = (HttpRequest) msg;

            boolean keepAlive = HttpUtil.isKeepAlive(req);
            CommonProto.Metadata message = CommonProto.Metadata.newBuilder()
                    .setBodyLength(1)
                    .build();
            byte[] messageBytes = message.toByteArray();
            ByteBuffer combinedBuffer = ByteBuffer.allocate(payload.length + messageBytes.length);
            combinedBuffer.put(payload);
            combinedBuffer.put(messageBytes);
            FullHttpResponse response = new DefaultFullHttpResponse(req.protocolVersion(), OK,
                    Unpooled.wrappedBuffer(combinedBuffer));
            response.headers()
                    .set(CONTENT_TYPE, "application/x-protobuf")
                    .setInt(CONTENT_LENGTH, messageBytes.length) // response.content().readableBytes())
                    .set(CONNECTION, KEEP_ALIVE);

            if (keepAlive) {
                if (!req.protocolVersion().isKeepAliveDefault()) {
                    response.headers().set(CONNECTION, KEEP_ALIVE);
                }
            } else {
                // Tell the client we're going to close the connection.
                response.headers().set(CONNECTION, CLOSE);
            }

            ChannelFuture f = ctx.write(response);

            if (!keepAlive) {
                f.addListener(ChannelFutureListener.CLOSE);
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}
