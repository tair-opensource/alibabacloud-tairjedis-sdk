# alibabacloud-tairjedis-sdk

<div align=center>
<img src="logo.png" width="500"/>
</div>

基于 [Jedis](https://github.com/xetorthio/jedis) 封装的，操作 [Tair](https://help.aliyun.com/document_detail/145957.html) 的客户端，支持 Tair For Redis 多种 [Module](https://help.aliyun.com/document_detail/146579.html) 的操作命令及部分高级特性。
- [TairHash](https://help.aliyun.com/document_detail/145970.html), 可实现 field 级别的过期。(已[开源](https://github.com/alibaba/TairHash))
- [TairString](https://help.aliyun.com/document_detail/145902.html), 支持 string 设置 version，增强的`cas`和`cad`命令可轻松实现分布式锁。(已[开源](https://github.com/alibaba/TairString))
- [TairZset](https://help.aliyun.com/document_detail/292812.html), 支持多维排序。(已[开源](https://github.com/alibaba/TairZset))
- [TairDoc](https://help.aliyun.com/document_detail/145940.html), 支持存储`JSON`类型。
- [TairGis](https://help.aliyun.com/document_detail/145971.html), 支持地理位置点、线、面的相交、包含等关系判断。（已[开源](https://github.com/tair-opensource/TairGis)）
- [TairBloom](https://help.aliyun.com/document_detail/145972.html), 支持动态扩容的布隆过滤器。
- [TairRoaring](https://help.aliyun.com/document_detail/311433.html), Roaring Bitmap, 使用少量的存储空间来实现海量数据的查询优化。
- [TairTs](https://help.aliyun.com/document_detail/408954.html), 时序数据结构，提供低时延、高并发的内存读写访问。
- [TairCpc](https://help.aliyun.com/document_detail/410587.html), 基于CPC（Compressed Probability Counting）压缩算法开发的数据结构，支持仅占用很小的内存空间对采样数据进行高性能计算。
- [TairSearch](https://help.aliyun.com/document_detail/417908.html), 支持ES-LIKE语法的全文索引和搜索模块。
- [TairVector](https://help.aliyun.com/document_detail/457193.html), 一个向量搜索数据结构，具有简单、灵活、实时、高效的特点。
# 快速开始

```
<dependency>
  <groupId>com.aliyun.tair</groupId>
  <artifactId>alibabacloud-tairjedis-sdk</artifactId>
  <version>（建议使用最新版本）</version>
</dependency>
```

最新版本查阅：[这里](https://s01.oss.sonatype.org/#nexus-search;quick~alibabacloud-tairjedis-sdk)  
JavaDoc地址：[这里](https://javadoc.io/doc/com.aliyun.tair/alibabacloud-tairjedis-sdk/latest/index.html)
## 如何选择版本
1. 3.x版本与5.x版本的区别是什么？
> 3.x版本依赖Jedis 3.6.0版本，5.x版本依赖[valkey-java](https://github.com/valkey-io/valkey-java)的5.3.0版本，两者之间存在不兼容，详见[3to5-CN.md](https://github.com/tair-opensource/alibabacloud-tairjedis-sdk/blob/master/docs/3to5-CN.md)。  
2. 我应该如何选择版本?
> 如果您是新用户，建议直接使用最新的 5.x 版本，如果您已经使用了3.x版本，可以继续使用3.x最新版本，我们会保证功能相关的代码移植回去。
# Example
参考 [tests/example/*](https://github.com/alibaba/alibabacloud-tairjedis-sdk/tree/master/src/test/java/com/aliyun/tair/tests/example) 下完整示例。

# 最佳实践
- [Redis客户端重试指南](https://help.aliyun.com/document_detail/303129.html)
- [JedisPool资源池优化](https://help.aliyun.com/document_detail/98726.html)  
- [基于TairGIS轻松实现用户轨迹监测](https://help.aliyun.com/document_detail/163537.html)
- [基于TairString实现高性能分布式锁](https://help.aliyun.com/document_detail/146758.html)
- [基于TairString实现高效的限流器（Bounded Counter）](https://help.aliyun.com/document_detail/147113.html)
- [基于TairZset轻松实现多维排行榜](https://help.aliyun.com/document_detail/313857.html)
- [基于TairZset实现分布式架构排行榜](https://help.aliyun.com/document_detail/356661.html)
- [基于TairRoaring实现的人群圈选方案](https://help.aliyun.com/document_detail/311920.html)
- [TairBloom的原理与最佳实践](https://help.aliyun.com/document_detail/145972.html)

# Tair 所有的 SDK

| language | GitHub |
|----------|---|
| Java     |https://github.com/alibaba/alibabacloud-tairjedis-sdk|
| Python   |https://github.com/alibaba/tair-py|
| Go       |https://github.com/alibaba/tair-go|
| .Net     |https://github.com/alibaba/AlibabaCloud.TairSDK|

# 依赖
tairjedis中TairSearch模块依赖: [OpenSearch](https://github.com/opensearch-project/OpenSearch)
