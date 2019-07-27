package com.github.sioncheng.namesvr;

import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.BiConsumer;

public class NameServerClientTest {

    @Test
    public void testConnect() throws Exception {
        NameServerClient nameServerClient = new NameServerClient();
        boolean connected = nameServerClient.connect();

        Assert.assertTrue(connected);

        ThreadUtil.sleepQuiet(1000);

        nameServerClient.putKVConfig("test-namespace", "t1", "v1").whenComplete(new BiConsumer<RemotingCommand, Throwable>() {
            public void accept(RemotingCommand remotingCommand, Throwable throwable) {
                if (remotingCommand != null) {
                    System.out.println(remotingCommand.toString());
                }
                if (throwable != null) {
                    throwable.printStackTrace();
                }
            }
        });

        ThreadUtil.sleepQuiet(1000);

        boolean closed = nameServerClient.close();

        Assert.assertTrue(closed);

        ThreadUtil.sleepQuiet(11000);

    }
}
