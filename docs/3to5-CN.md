本文档描述 alibabacloud-tairjedis-sdk 从 3.x 版本升级到 5.x 版本带来的 break changes。

1. 本次升级将底层的依赖从 [Jedis](https://github.com/redis/jedis) 改为了 [Valkey-Java](https://github.com/valkey-io/valkey-java)，它是一个从 Jedis Fork 而来的替代品。因此您需要将`redis.clients.jedis.`更改为`io.valkey.`

2. 由于 Jedis [4.x](https://github.com/redis/jedis/blob/master/docs/3to4.md) 和 [5.x](https://github.com/redis/jedis/blob/master/docs/breaking-5.md) 带来了许多不兼容升级，Valkey-java 从 Jedis Fork 而来，或许您可能要改某些 API 的返回值，详见文档。

3. 因为 Valkey-java 新版本删除了这些类，因此需要修改 import 的路径。
- `import redis.clients.jedis.ScanParams;` to `import com.aliyun.tair.jedis3.ScanParams;`
- `import redis.clients.jedis.ScanResult;` to `import com.aliyun.tair.jedis3.ScanResult;`
- `import redis.clients.jedis.GeoUnit;` to `import redis.clients.jedis.args.GeoUnit;`
- `import redis.clients.jedis.params.Params;` to `import com.aliyun.tair.jedis3.Params;`

4. 一些被 Jedis 删除的方法需要使用 Jedis3BuilderFactory。
- `BuilderFactory.BYTE_ARRAY_MAP` to `Jedis3BuilderFactory.BYTE_ARRAY_MAP`

5. Pipeline 的使用方法发生了改变。

原始的如下：
```java
TairHashPipeline pipeline = new TairHashPipeline();
pipeline.setClient(jedis.getClient());
pipeline.set("xx", "yy");
pipeline.sync();
```
新的如下：
```java
TairHashPipeline pipeline = new TairHashPipeline(jedis);
pipeline.set("xx", "yy");
pipeline.sync();
```