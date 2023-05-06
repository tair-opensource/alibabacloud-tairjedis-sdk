# alibabacloud-tairjedis-sdk

<div align=center>
<img src="logo.png" width="500"/>
</div>

English | [简体中文](README-CN.md)

A client packaged based on [Jedis](https://github.com/xetorthio/jedis) that operates [Tair](https://www.alibabacloud.com/help/en/apsaradb-for-redis/latest/apsaradb-for-redis-enhanced-edition-overview) For Redis Modules.

- [TairHash](https://www.alibabacloud.com/help/en/apsaradb-for-redis/latest/tairhash-commands), is a hash that allows you to specify the expiration time and version number of a field. ([Open sourced](https://github.com/alibaba/TairHash))
- [TairString](https://www.alibabacloud.com/help/en/apsaradb-for-redis/latest/tairstring-commands), is a string that contains a version number. ([Open sourced](https://github.com/alibaba/TairString))
- [TairZset](https://www.alibabacloud.com/help/en/apsaradb-for-redis/latest/tairzset-commands), allows you to sort data of the double type based on multiple dimensions. ([Open sourced](https://github.com/alibaba/TairZset))
- [TairDoc](https://www.alibabacloud.com/help/en/apsaradb-for-redis/latest/tairdoc-commands), to perform create, read, update, and delete (CRUD) operations on JSON data. 
- [TairGis](https://www.alibabacloud.com/help/en/apsaradb-for-redis/latest/tairgis-commands), allowing you to query points, linestrings, and polygons. ([Open Sourced](https://github.com/tair-opensource/TairGis))
- [TairBloom](https://www.alibabacloud.com/help/en/apsaradb-for-redis/latest/tairbloom-commands), is a Bloom filter that supports dynamic scaling. 
- [TairRoaring](https://www.alibabacloud.com/help/en/apsaradb-for-redis/latest/tairroaring-commands), is a more efficient and balanced type of compressed bitmaps recognized by the industry. 
- [TairTs](https://www.alibabacloud.com/help/en/apsaradb-for-redis/latest/tairts-commands), is a time series data structure that is developed on top of Redis modules.  
- [TairCpc](https://www.alibabacloud.com/help/en/apsaradb-for-redis/latest/taircpc-commands), is a data structure developed based on the compressed probability counting (CPC) sketch. 
- [TairSearch](https://www.alibabacloud.com/help/en/apsaradb-for-redis/latest/tairsearch-command), is a full-text search module developed in-house based on Redis modules. 
- [TairVector](https://www.alibabacloud.com/help/en/apsaradb-for-redis/latest/tairvector), is a vector search data structure, offering simplicity, flexibility, real-time performance, and high efficiency.

# Quick start
```
<dependency>
  <groupId>com.aliyun.tair</groupId>
  <artifactId>alibabacloud-tairjedis-sdk</artifactId>
  <version>Latest version</version>
</dependency>
```

The latest verison：[here](https://s01.oss.sonatype.org/#nexus-search;quick~alibabacloud-tairjedis-sdk)  
JavaDoc: [here](https://javadoc.io/doc/com.aliyun.tair/alibabacloud-tairjedis-sdk/latest/index.html)

# Example
Refer to the complete example under [tests/example/*](https://github.com/alibaba/alibabacloud-tairjedis-sdk/tree/master/src/test/java/com/aliyun/tair/tests/example)

# Best Practices
- [Monitor user trajectories by using TairGIS](https://www.alibabacloud.com/help/en/apsaradb-for-redis/latest/monitor-user-trajectories-by-using-tairgis)
- [Implement high-performance distributed locks by using TairString](https://www.alibabacloud.com/help/en/apsaradb-for-redis/latest/implement-high-performance-distributed-locks-by-using-tairstring)
- [Implement bounded counters by using TairString](https://www.alibabacloud.com/help/en/apsaradb-for-redis/latest/implement-bounded-counters-by-using-tairstring)
- [Implement multidimensional leaderboards by using TairZset](https://www.alibabacloud.com/help/en/apsaradb-for-redis/latest/implement-multidimensional-leaderboards-by-using-tairzset)
- [Implement fine-grained monitoring by using TairTS](https://www.alibabacloud.com/help/en/apsaradb-for-redis/latest/implement-fine-grained-monitoring-by-using-tairts)
- [Implement distributed leaderboards by using TairZset](https://www.alibabacloud.com/help/en/apsaradb-for-redis/latest/implement-distributed-leaderboards-by-using-tairzset)
- [Select users by using TairRoaring](https://www.alibabacloud.com/help/en/apsaradb-for-redis/latest/select-users-by-using-tairroaring)

# Tair All SDK

| language | GitHub |
|----------|---|
| Java     |https://github.com/alibaba/alibabacloud-tairjedis-sdk|
| Python   |https://github.com/alibaba/tair-py|
| Go       |https://github.com/alibaba/tair-go|
| .Net     |https://github.com/alibaba/AlibabaCloud.TairSDK|

# Dependency
The TairSearch module in tairjedis depends on: [OpenSearch](https://github.com/opensearch-project/OpenSearch)
