![](https://img.alicdn.com/tfs/TB1Ly5oS3HqK1RjSZFPXXcwapXa-238-54.png)

# 概述

alibabacloud-tairjedis-sdk是基于[Jedis](https://github.com/xetorthio/jedis)封装的，操作[云数据库Redis企业版](https://help.aliyun.com/document_detail/146579.html) 的客户端，主要包含下列功能：

- 支持企业版多种[Module](https://help.aliyun.com/document_detail/146579.html)的操作命令

# 安装方法

```
<dependency>
  <groupId>com.aliyun.tair</groupId>
  <artifactId>alibabacloud-tairjedis-sdk</artifactId>
  <version>1.0.0（建议使用最新版本）</version>
</dependency>
```

最新版本查阅：[这里](https://oss.sonatype.org/#nexus-search;quick~alibabacloud-tairjedis-sdk)

# 快速使用

文档地址：[这里](https://javadoc.io/doc/com.aliyun.tair/alibabacloud-tairjedis-sdk/latest/index.html)

初始化
```
public class TestBase {
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 6379;
    private static final int CLUSTER_PORT = 30001;

    private static Jedis jedis;
    private static JedisCluster jedisCluster;

    public static TairDoc tairDoc;
    public static TairDocPipeline tairDocPipeline;
    public static TairDocCluster tairDocCluster;

    @BeforeClass
    public static void setUp() {
        if (jedis == null) {
            jedis = new Jedis(HOST, PORT);
            if (!"PONG".equals(jedis.ping())) {
                System.exit(-1);
            }

            Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
            jedisClusterNodes.add(new HostAndPort(HOST, CLUSTER_PORT));
            jedisCluster = new JedisCluster(jedisClusterNodes);

            tairDoc = new TairDoc(jedis);
            tairDocPipeline = new TairDocPipeline();
            tairDocPipeline.setClient(jedis.getClient());
            tairDocCluster = new TairDocCluster(jedisCluster);
        }
    }

    @AfterClass
    public static void tearDown() {
        if (jedis != null) {
            jedis.close();
        }
    }
}
```

普通命令：
```
    @Test
    public void jsonSetTest() {
        String ret = tairDoc.jsonset(jsonKey, ".", JSON_STRING_EXAMPLE);
        assertEquals("OK", ret);

        ret = tairDoc.jsonget(jsonKey, ".");
        assertEquals(JSON_STRING_EXAMPLE, ret);

        ret = tairDoc.jsonget(jsonKey, ".foo");
        assertEquals("\"bar\"", ret);

        ret = tairDoc.jsonget(jsonKey, ".baz");
        assertEquals("42", ret);
    }
```

Pipeline
```
    @Test
    public void jsonSetPipelineTest() {
        tairDocPipeline.jsonset(jsonKey, ".", JSON_STRING_EXAMPLE);
        tairDocPipeline.jsonget(jsonKey, ".");
        tairDocPipeline.jsonget(jsonKey, ".foo");
        tairDocPipeline.jsonget(jsonKey, ".baz");

        List<Object> objs = tairDocPipeline.syncAndReturnAll();

        assertEquals("OK", objs.get(0));
        assertEquals(JSON_STRING_EXAMPLE, objs.get(1));
        assertEquals("\"bar\"", objs.get(2));
        assertEquals("42", objs.get(3));
    }
```

Cluster
```
    @Test
    public void jsonSetClusterTest() {
        String ret = tairDocCluster.jsonset(jsonKey, jsonKey, ".", JSON_STRING_EXAMPLE);
        assertEquals("OK", ret);

        ret = tairDocCluster.jsonget(jsonKey, jsonKey, ".");
        assertEquals(JSON_STRING_EXAMPLE, ret);

        ret = tairDocCluster.jsonget(jsonKey, jsonKey, ".foo");
        assertEquals("\"bar\"", ret);

        ret = tairDocCluster.jsonget(jsonKey, jsonKey, ".baz");
        assertEquals("42", ret);
    }
```

# 代码示例

## 分布式锁

加锁：redis set with nx，and expire time  
解锁：cad(compare and delete) instead of lua
```
public class DistributeLock {
    private TairString tairString;
    private Jedis jedis;

    public DistributeLock(Jedis j) {
        jedis = j;
        tairString = new TairString(j);
    }

    public boolean tryGetDistributedLock(String lockKey, String requestId, int expireTime) {
        try {
            String result = jedis.set(lockKey, requestId, SetParams.setParams().nx().ex(expireTime));
            if ("OK".equals(result)) {
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public boolean releaseDistributedLock(String lockKey, String requestId) {
        try {
            Long ret = tairString.cad(lockKey, requestId);
            if (1 == ret) {
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
}
```

## TairDoc示例

将学生信息存储为JSON格式，直接通过path更新部分JSON对象，并给年龄+1。
```
public class TairDocTest {
    private static TairDoc tairDoc;

    public static void main(String[] args) {
        Jedis jedis = new Jedis("127.0.0.1", 6379);
        tairDoc = new TairDoc(jedis);

        Student student = new Student("Tom", 18);

        // 存储
        tairDoc.jsonset("tominfo", ".", JSON.toJSONString(student));

        // 更新姓名为Danny, 年龄+1
        tairDoc.jsonset("tominfo", ".name", "\"Danny\"");
        tairDoc.jsonnumincrBy("tominfo", ".age", 2.0);

        // 重新获取
        String tomInfo = tairDoc.jsonget("tominfo");
        System.out.println(tomInfo);

        jedis.close();
    }

    static class Student {
        private String name;
        private int age;

        Student(String name, int age) {
            this.name = name;
            this.age = age;
        }

        public String getName() {
            return name;
        }

        public int getAge() {
            return age;
        }
    }
}
```
