package com.aliyun.tair.tairzset.params;

import java.util.ArrayList;

import com.aliyun.tair.jedis3.Params;
import io.valkey.util.SafeEncoder;

public class ExzaddParams extends Params {

  private static final String XX = "xx";
  private static final String NX = "nx";
  private static final String CH = "ch";
  private static final String INCR = "incr";

  public ExzaddParams() {
  }

  public static ExzaddParams ExzaddParams() {
    return new ExzaddParams();
  }

  /**
   * Only set the key if it does not already exist.
   * @return ExzaddParams
   */
  public ExzaddParams nx() {
    addParam(NX);
    return this;
  }

  /**
   * Only set the key if it already exist.
   * @return ExzaddParams
   */
  public ExzaddParams xx() {
    addParam(XX);
    return this;
  }

  /**
   * Modify the return value from the number of new elements added to the total number of elements
   * changed
   * @return ExzaddParams
   */
  public ExzaddParams ch() {
    addParam(CH);
    return this;
  }

  public ExzaddParams incr() {
    addParam(INCR);
    return this;
  }

  public byte[][] getByteParams(byte[] key, byte[]... args) {
    ArrayList<byte[]> byteParams = new ArrayList<byte[]>();
    byteParams.add(key);

    if (contains(NX)) {
      byteParams.add(SafeEncoder.encode(NX));
    }
    if (contains(XX)) {
      byteParams.add(SafeEncoder.encode(XX));
    }
    if (contains(CH)) {
      byteParams.add(SafeEncoder.encode(CH));
    }
    if (contains(INCR)) {
      byteParams.add(SafeEncoder.encode(INCR));
    }

    for (byte[] arg : args) {
      byteParams.add(arg);
    }

    return byteParams.toArray(new byte[byteParams.size()][]);
  }

}
