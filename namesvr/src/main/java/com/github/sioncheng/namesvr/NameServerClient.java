package com.github.sioncheng.namesvr;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.header.namesrv.PutKVConfigRequestHeader;
import org.apache.rocketmq.remoting.netty.NettyDecoder;
import org.apache.rocketmq.remoting.netty.NettyEncoder;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class NameServerClient {

    private String host;
    private int port;
    private Bootstrap bootstrap;
    private Channel channel;
    private NameServerClientHandler clientHandler;

    private static Logger logger = LoggerFactory.getLogger(NameServerClient.class);
    private static ConcurrentHashMap<Integer, CompletableFutureTask> reqRes =
            new ConcurrentHashMap<>();
    private static ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor ( (r)->{
        Thread thread = new Thread(r);
        thread.setDaemon(true);
        thread.setName("name-server-client-thread-0");

        return thread;
    });

    public NameServerClient() {
        this("localhost", 9876);
    }

    public NameServerClient(String host,int port) {
        this.host = host;
        this.port = port;
    }

    public boolean connect() throws InterruptedException {
        bootstrap = new Bootstrap();
        bootstrap.group(new NioEventLoopGroup(1));
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.handler(new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                logger.info(String.format("connect to %s:%d", host, port));

                ChannelPipeline channelPipeline = ch.pipeline();
                channelPipeline.addLast(new NettyEncoder());
                channelPipeline.addLast(new NettyDecoder());
                clientHandler = new NameServerClientHandler();
                channelPipeline.addLast(clientHandler);
            }
        });

        ChannelFuture channelFuture = bootstrap.connect(host, port);
        channel = channelFuture.sync().channel();

        tryCheckTimeout();

        return channel.isOpen();
    }

    public CompletableFuture<RemotingCommand> putKVConfig(String namespace, String key, String value) {
        PutKVConfigRequestHeader requestHeader = new PutKVConfigRequestHeader();
        requestHeader.setNamespace(namespace);
        requestHeader.setKey(key);
        requestHeader.setValue(value);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PUT_KV_CONFIG, requestHeader);

        clientHandler.sendCommand(request);

        CompletableFuture<RemotingCommand> commandCompletableFuture = new CompletableFuture<RemotingCommand>();

        reqRes.put(request.getOpaque(), new CompletableFutureTask(commandCompletableFuture));

        return commandCompletableFuture;
    }

    public boolean close() {
        if (channel != null) {
            channel.close();
            channel = null;
        }

        return  true;
    }

    private void tryCheckTimeout() {
        executorService.scheduleAtFixedRate (()-> {
            List<Integer> timeoutResList = new LinkedList<>();

            for (Map.Entry<Integer, CompletableFutureTask> kv :
                    reqRes.entrySet()) {
                if (System.currentTimeMillis() - kv.getValue().createdTime > 10000) {
                    kv.getValue().res.completeExceptionally(new Exception("timeout"));
                    timeoutResList.add(kv.getKey());
                }
            }

            timeoutResList.forEach(v -> reqRes.remove(v));


        }, 10, 10, TimeUnit.SECONDS);

    }

    public static class NameServerClientHandler extends SimpleChannelInboundHandler<RemotingCommand> {

        private static final Logger logger = LoggerFactory.getLogger(NameServerClientHandler.class);

        private ChannelHandlerContext channelHandlerContext;

        public void sendCommand(RemotingCommand remotingCommand) {
            channelHandlerContext.writeAndFlush(remotingCommand);
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            logger.info("channel read0");
            logger.info(msg.toString());
            executorService.execute(() -> {
                    CompletableFutureTask req = reqRes.remove(msg.getOpaque());
                    if (req != null) {
                        req.res.complete(msg);
                    }
                }
            );
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            super.channelActive(ctx);
            this.channelHandlerContext = ctx;
            logger.info("channel active");
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            super.channelInactive(ctx);
        }
    }

    private static class CompletableFutureTask {
        private final long createdTime;
        private final CompletableFuture<RemotingCommand> res;

        public CompletableFutureTask(CompletableFuture<RemotingCommand> res) {
            this.res = res;
            this.createdTime = System.currentTimeMillis();
        }
    }
}
