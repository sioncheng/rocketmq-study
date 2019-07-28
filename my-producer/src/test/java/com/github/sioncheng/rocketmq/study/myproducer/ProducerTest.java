package com.github.sioncheng.rocketmq.study.myproducer;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;


public class ProducerTest {

    @Test
    public void hello() {
        Assert.assertTrue(1 == 1);
    }

    @Test
    public void testStart() {
        Producer producer = new Producer(Arrays.asList("localhost:9876"), "");
        try {
            producer.start();
        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        }
    }
}
