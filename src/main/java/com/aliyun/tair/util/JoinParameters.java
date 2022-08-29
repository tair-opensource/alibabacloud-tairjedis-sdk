package com.aliyun.tair.util;

public class JoinParameters {
    public static byte[][] joinParameters(byte[] first, byte[][] rest) {
        byte[][] result = new byte[rest.length + 1][];
        result[0] = first;
        System.arraycopy(rest, 0, result, 1, rest.length);
        return result;
    }

    public static String[] joinParameters(String first, String[] rest) {
        String[] result = new String[rest.length + 1];
        result[0] = first;
        System.arraycopy(rest, 0, result, 1, rest.length);
        return result;
    }

    public static byte[][] joinParameters(byte[] first, byte[] second, byte[][] rest) {
        byte[][] result = new byte[rest.length + 2][];
        result[0] = first;
        result[1] = second;
        System.arraycopy(rest, 0, result, 2, rest.length);
        return result;
    }

    public static String[] joinParameters(String first, String second, String[] rest) {
        String[] result = new String[rest.length + 2];
        result[0] = first;
        result[1] = second;
        System.arraycopy(rest, 0, result, 2, rest.length);
        return result;
    }

    public static byte[][] joinParameters(byte[] first, byte[] second, byte[] third, byte[][] rest) {
        byte[][] result = new byte[rest.length + 3][];
        result[0] = first;
        result[1] = second;
        result[2] = third;
        System.arraycopy(rest, 0, result, 3, rest.length);
        return result;
    }

    public static byte[][] joinParameters(byte[] first, byte[] second, byte[] third, byte[] fourth, byte[][] rest) {
        byte[][] result = new byte[rest.length + 4][];
        result[0] = first;
        result[1] = second;
        result[2] = third;
        result[3] = fourth;
        System.arraycopy(rest, 0, result, 4, rest.length);
        return result;
    }
}
