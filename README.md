# alibabacloud-tairjedis-sdk

基于 [Jedis](https://github.com/xetorthio/jedis) 封装的，操作 [云数据库Redis企业版](https://help.aliyun.com/document_detail/145957.html) 的客户端，支持企业版多种 [Module](https://help.aliyun.com/document_detail/146579.html) 的操作命令。

# 安装方法

```
<dependency>
  <groupId>com.aliyun.tair</groupId>
  <artifactId>alibabacloud-tairjedis-sdk</artifactId>
  <version>（建议使用最新版本）</version>
</dependency>
```

最新版本查阅：[这里](https://mvnrepository.com/artifact/com.aliyun.tair/alibabacloud-tairjedis-sdk)  
JavaDoc地址：[这里](https://javadoc.io/doc/com.aliyun.tair/alibabacloud-tairjedis-sdk/latest/index.html)

# 代码实例

## 分布式锁

原理介绍见 [高性能分布式锁](https://help.aliyun.com/document_detail/146758.html) 

加锁：Redis SET with NX and EX，Redis原生命令。  
解锁：CAD(compare and delete)，阿里云Redis企业版特有命令，替代原生Redis Lua方案。

```
public static boolean releaseDistributedLock(String lockKey, String requestId) {
    Jedis jedis = null;
    try {
        jedis = jedisPool.getResource();
        TairString tairString = new TairString(jedis);
        Long ret = tairString.cad(lockKey, requestId);
        if (1 == ret) {
            return true;
        }
    } catch (Exception e) {
        e.printStackTrace();
    } finally {
        if (jedis != null) {
            jedis.close();
        }
    }
    return false;
}
```

完整代码见：[这里](https://github.com/aliyun/alibabacloud-tairjedis-sdk/blob/master/src/test/java/com/aliyun/tair/tests/example/DistributeLock.java)

执行结果如下，可以看到10个线程分别对total相加10次之后，得到的结果是100，说明分布式锁成功限制了线程并发的情况。

```
...
I am thread: Thread-8, unlock success, total: 98
I am thread: Thread-5, lock success, total: 98
I am thread: Thread-5, unlock success, total: 99
I am thread: Thread-7, lock success, total: 99
I am thread: Thread-7, unlock success, total: 100
Final total is: 100
```

## 通过终端操作TairDoc

1，打开 https://shell.aliyun.com/ ，您将获得一个终端。

2，执行下面命令
> cloudshell-git-open https://code.aliyun.com/redisuser/tairdoc-tutorial.git;teachme tutorial.md

开始进行演示操作，右边有完整的示例过程，您可以点击`执行命令`，命令将被拷贝到左边的shell上。

3，通过教程，您可以直接执行企业版的TairDoc命令。

![](https://raw.githubusercontent.com/aliyun/alibabacloud-tairjedis-sdk/master/assets/tairdoc.jpg)
