本文档描述 alibabacloud-tairjedis-sdk 从 3.x 版本升级到 5.x 版本带来的破坏性改变，由于 Jedis [4.x](https://github.com/redis/jedis/blob/master/docs/3to4.md) 和 [5.x](https://github.com/redis/jedis/blob/master/docs/breaking-5.md) 带来了许多不兼容升级，alibabacloud-tairjedis-sdk 本身依赖 Jedis，因此破坏性改变无法避免，但我们的适配原则是尽量减少用户的代码改动。

1. 依赖的类需要修改 import 的路径
  - `import redis.clients.jedis.ScanParams;` to `import com.aliyun.tair.jedis3.ScanParams;`
  - `import redis.clients.jedis.ScanResult;` to `import com.aliyun.tair.jedis3.ScanResult;`
  - `import redis.clients.jedis.GeoUnit;` to `import redis.clients.jedis.args.GeoUnit;`
  - `import redis.clients.jedis.params.Params;` to `import com.aliyun.tair.jedis3.Params;`

2. 一些被 Jedis 删除的方法需要使用 Jedis3BuilderFactory
  - `BuilderFactory.BYTE_ARRAY_MAP` to `Jedis3BuilderFactory.BYTE_ARRAY_MAP`

3. Pipeline 的使用方法发生了改变

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
