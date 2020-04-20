package com.aliyun.tair.tests.util;

import com.aliyun.tair.util.JoinParameters;
import org.junit.Assert;
import org.junit.Test;

public class JoinParametersTest {
    @Test
    public void test1() {
        String first = "0";
        String[] rest = new String[] {"1", "2", "3"};
        String[] strings = JoinParameters.joinParameters(first, rest);
        for (int i = 0; i < strings.length; i++) {
            Assert.assertEquals(String.valueOf(i), strings[i]);
        }
    }

    @Test
    public void test2() {
        byte[] first = "0".getBytes();
        byte[][] rest = new byte[][] {"1".getBytes(), "2".getBytes(), "3".getBytes()};
        byte[][] bytes = JoinParameters.joinParameters(first, rest);
        for (int i = 0; i < bytes.length; i++) {
            Assert.assertArrayEquals(String.valueOf(i).getBytes(), bytes[i]);
        }
    }
}
