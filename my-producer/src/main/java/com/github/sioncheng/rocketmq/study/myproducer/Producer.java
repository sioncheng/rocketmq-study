package com.github.sioncheng.rocketmq.study.myproducer;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class Producer {

    private List<String> nameServers;
    private String topic;
    private NettyRemotingClient nameServerClient;
    private ClusterInfo clusterInfo;

    private ExecutorService executorService = Executors.newFixedThreadPool(1, new ThreadFactory() {
        public Thread newThread(Runnable r) {
            Thread t = new Thread();
            t.setName("producer-thread-0");

            return t;
        }
    });

    public Producer(List<String> nameServers, String topic) {
        this.nameServers = nameServers;
        this.topic = topic;
    }

    public void start() throws ProducerException {
        initNameServerClient();
        fetchBrokerList();
    }

    public void stop() {

    }

    private void initNameServerClient() {
        NettyClientConfig nettyClientConfig = new NettyClientConfig();
        nameServerClient = new NettyRemotingClient(nettyClientConfig, new ChannelEventListener() {
            public void onChannelConnect(String s, Channel channel) {

            }

            public void onChannelClose(String s, Channel channel) {

            }

            public void onChannelException(String s, Channel channel) {

            }

            public void onChannelIdle(String s, Channel channel) {

            }
        });
        nameServerClient.start();
    }

    private void fetchBrokerList() throws ProducerException {
        Throwable ex = null;
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CLUSTER_INFO, null);
        for (String nameServer :
                nameServers) {
            try {
                RemotingCommand response = nameServerClient.invokeSync(nameServer, request, 2000);
                if (response.getCode() == ResponseCode.SUCCESS) {
                    clusterInfo = ClusterInfo.decode(response.getBody(), ClusterInfo.class);
                    break;
                }
            } catch (Exception e) {
                ex = e;
            }
        }

        if (clusterInfo == null) {
            throw new ProducerException(ProducerException.ExceptionCode.FETCH_CLUSTER_INFO_FAILURE,
                    "unable to fetch broker info", ex);
        }
    }

    static class NameServerProcessor implements NettyRequestProcessor {
        public RemotingCommand processRequest(ChannelHandlerContext channelHandlerContext, RemotingCommand remotingCommand) throws Exception {
            return null;
        }

        public boolean rejectRequest() {
            return false;
        }
    }
}
