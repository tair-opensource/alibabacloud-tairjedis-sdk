package com.aliyun.tair.jedis3;

import io.valkey.util.SafeEncoder;

public enum GeoUnit {
  M, KM, MI, FT;

  public final byte[] raw;

  GeoUnit() {
    raw = SafeEncoder.encode(this.name().toLowerCase());
  }
}
