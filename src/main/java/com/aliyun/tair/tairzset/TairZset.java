package com.aliyun.tair.tairzset;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.tairzset.params.ExzaddParams;
import com.aliyun.tair.tairzset.params.ExzrangeParams;
import com.aliyun.tair.util.JoinParameters;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.util.SafeEncoder;

import static com.aliyun.tair.tairzset.LeaderBoard.joinScoresToString;
import static redis.clients.jedis.Protocol.toByteArray;

public class TairZset {
    private Jedis jedis;

    public TairZset(Jedis jedis) {
        this.jedis = jedis;
    }

    private Jedis getJedis() {
        return jedis;
    }

    /**
     * Adds all the specified members with the specified (multi)scores to the tairzset stored at key.
     * @param key
     * @param member
     * @param scores
     * @return
     */
    public Long exzadd(final String key, final String member, final double... scores) {
        return exzadd(SafeEncoder.encode(key), SafeEncoder.encode(joinScoresToString(scores)),
            SafeEncoder.encode(member));
    }
    public Long exzadd(final byte[] key, final byte[] member, final double... scores) {
        return exzadd(key, SafeEncoder.encode(joinScoresToString(scores)), member);
    }

    public Long exzadd(final String key, final String score, final String member) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZADD, key, score, member);
        return BuilderFactory.LONG.build(obj);
    }

    public Long exzadd(final byte[] key, final byte[] score, final byte[] member) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZADD, key, score, member);
        return BuilderFactory.LONG.build(obj);
    }

    public Long exzadd(final String key, final String score, final String member, final ExzaddParams params) {
        return exzadd(SafeEncoder.encode(key), SafeEncoder.encode(score), SafeEncoder.encode(member), params);
    }

    public Long exzadd(final byte[] key, final byte[] score, final byte[] member, final ExzaddParams params) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZADD, params.getByteParams(key, score, member));
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * There is a bug in this method: the order of scoreMembers is score first and member last,
     * which makes it impossible to store members with the same score.
     * see {@link TairZset#exzaddMembers(String, Map)}
     */
    @Deprecated
    public Long exzadd(final String key, final Map<String, String> scoreMembers) {
        final List<byte[]> bparams = new ArrayList<byte[]>();
        bparams.add(SafeEncoder.encode(key));

        for (final Entry<String, String> entry : scoreMembers.entrySet()) {
            bparams.add(SafeEncoder.encode(entry.getKey()));
            bparams.add(SafeEncoder.encode(entry.getValue()));
        }
        Object obj = getJedis().sendCommand(ModuleCommand.EXZADD, bparams.toArray(new byte[bparams.size()][]));
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * Add multiple scores and members to zset.
     * @param key the key
     * @param members a map, key is member, value is score.
     * @return When used without ExzaddParams, the number of elements added to the sorted set (excluding score updates).
     * If the CH option is specified, the number of elements that were changed (added or updated).
     * If the INCR option is specified, the return value will be Bulk string reply:
     * The new score of member (a double precision floating point number) represented as string, or nil if the
     * operation was aborted (when called with either the XX or the NX option).
     */
    public Long exzaddMembers(final String key, final Map<String, String> members) {
        final List<byte[]> bparams = new ArrayList<byte[]>();
        bparams.add(SafeEncoder.encode(key));

        for (final Entry<String, String> entry : members.entrySet()) {
            bparams.add(SafeEncoder.encode(entry.getValue()));
            bparams.add(SafeEncoder.encode(entry.getKey()));
        }
        Object obj = getJedis().sendCommand(ModuleCommand.EXZADD, bparams.toArray(new byte[bparams.size()][]));
        return BuilderFactory.LONG.build(obj);
    }

    /** see {@link TairZset#exzaddMembers(byte[], Map)} */
    @Deprecated
    public Long exzadd(final byte[] key, final Map<byte[], byte[]> scoreMembers) {
        final List<byte[]> bparams = new ArrayList<byte[]>();
        bparams.add(key);

        for (final Entry<byte[], byte[]> entry : scoreMembers.entrySet()) {
            bparams.add(entry.getKey());
            bparams.add(entry.getValue());
        }
        Object obj = getJedis().sendCommand(ModuleCommand.EXZADD, bparams.toArray(new byte[bparams.size()][]));
        return BuilderFactory.LONG.build(obj);
    }

    public Long exzaddMembers(final byte[] key, final Map<byte[], byte[]> members) {
        final List<byte[]> bparams = new ArrayList<byte[]>();
        bparams.add(key);

        for (final Entry<byte[], byte[]> entry : members.entrySet()) {
            bparams.add(entry.getValue());
            bparams.add(entry.getKey());
        }
        Object obj = getJedis().sendCommand(ModuleCommand.EXZADD, bparams.toArray(new byte[bparams.size()][]));
        return BuilderFactory.LONG.build(obj);
    }

    /** see {@link TairZset#exzaddMembers(String, Map, ExzaddParams)} */
    @Deprecated
    public Long exzadd(final String key, final Map<String, String> scoreMembers, final ExzaddParams params) {
        final List<byte[]> bparams = new ArrayList<byte[]>();
        for (final Entry<String, String> entry : scoreMembers.entrySet()) {
            bparams.add(SafeEncoder.encode(entry.getKey()));
            bparams.add(SafeEncoder.encode(entry.getValue()));
        }
        Object obj = getJedis().sendCommand(ModuleCommand.EXZADD,
            params.getByteParams(SafeEncoder.encode(key), bparams.toArray(new byte[bparams.size()][])));
        return BuilderFactory.LONG.build(obj);
    }

    public Long exzaddMembers(final String key, final Map<String, String> members, final ExzaddParams params) {
        final List<byte[]> bparams = new ArrayList<byte[]>();
        for (final Entry<String, String> entry : members.entrySet()) {
            bparams.add(SafeEncoder.encode(entry.getValue()));
            bparams.add(SafeEncoder.encode(entry.getKey()));
        }
        Object obj = getJedis().sendCommand(ModuleCommand.EXZADD,
            params.getByteParams(SafeEncoder.encode(key), bparams.toArray(new byte[bparams.size()][])));
        return BuilderFactory.LONG.build(obj);
    }
    
    /** see {@link TairZsetCluster#exzaddMembers(byte[], Map, ExzaddParams)} */
    @Deprecated
    public Long exzadd(final byte[] key, final Map<byte[], byte[]> scoreMembers, final ExzaddParams params) {
        final List<byte[]> bparams = new ArrayList<byte[]>();
        for (final Entry<byte[], byte[]> entry : scoreMembers.entrySet()) {
            bparams.add(entry.getKey());
            bparams.add(entry.getValue());
        }
        Object obj = getJedis().sendCommand(ModuleCommand.EXZADD,
            params.getByteParams(key, bparams.toArray(new byte[bparams.size()][])));
        return BuilderFactory.LONG.build(obj);
    }

    public Long exzaddMembers(final byte[] key, final Map<byte[], byte[]> members, final ExzaddParams params) {
        final List<byte[]> bparams = new ArrayList<byte[]>();
        for (final Entry<byte[], byte[]> entry : members.entrySet()) {
            bparams.add(entry.getValue());
            bparams.add(entry.getKey());
        }
        Object obj = getJedis().sendCommand(ModuleCommand.EXZADD,
            params.getByteParams(key, bparams.toArray(new byte[bparams.size()][])));
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * Increments the score of member in the tairzset stored at key by increment.
     * @param key
     * @param increment
     * @param member
     * @return
     */
    public String exzincrBy(final String key, final String increment, final String member) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZINCRBY, key, increment, member);
        return BuilderFactory.STRING.build(obj);
    }

    public byte[] exzincrBy(final byte[] key, final byte[] increment, final byte[] member) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZINCRBY, key, increment, member);
        return BuilderFactory.BYTE_ARRAY.build(obj);
    }

    public String exzincrBy(final String key, final String member, final double... scores) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZINCRBY, key, joinScoresToString(scores), member);
        return BuilderFactory.STRING.build(obj);
    }

    public byte[] exzincrBy(final byte[] key, final byte[] member, final double... scores) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZINCRBY, key,
            SafeEncoder.encode(joinScoresToString(scores)), member);
        return BuilderFactory.BYTE_ARRAY.build(obj);
    }

    /**
     * Removes the specified members from the tairzset stored at key. Non existing members are ignored.
     * @param key
     * @param member
     * @return
     */
    public Long exzrem(final String key, final String... member) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZREM, JoinParameters.joinParameters(key, member));
        return BuilderFactory.LONG.build(obj);
    }

    public Long exzrem(final byte[] key, final byte[]... member) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZREM, JoinParameters.joinParameters(key, member));
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * Removes all elements in the tairzset stored at key with a score between min and max (inclusive).
     * @param key
     * @param min
     * @param max
     * @return
     */
    public Long exzremrangeByScore(final String key, final String min, final String max) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZREMRANGEBYSCORE, key, min, max);
        return BuilderFactory.LONG.build(obj);
    }

    public Long exzremrangeByScore(final byte[] key, final byte[] min, final byte[] max) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZREMRANGEBYSCORE, key, min, max);
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * Removes all elements in the tairzset stored at key with rank between start and stop.
     * @param key
     * @param start
     * @param stop
     * @return
     */
    public Long exzremrangeByRank(final String key, final long start, final long stop) {
        return exzremrangeByRank(SafeEncoder.encode(key), start, stop);
    }

    public Long exzremrangeByRank(final byte[] key, final long start, final long stop) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZREMRANGEBYRANK, key, toByteArray(start),
            toByteArray(stop));
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * When all the elements in a sorted set are inserted with the same score, in order to force lexicographical
     * ordering, this command removes all elements in the sorted set stored at key between the lexicographical range
     * specified by min and max.
     *
     * The meaning of min and max are the same of the ZRANGEBYLEX command. Similarly, this command actually removes
     * the same elements that ZRANGEBYLEX would return if called with the same min and max arguments.
     * @param key
     * @param min
     * @param max
     * @return
     */
    public Long exzremrangeByLex(final String key, final String min, final String max) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZREMRANGEBYLEX, key, min, max);
        return BuilderFactory.LONG.build(obj);
    }

    public Long exzremrangeByLex(final byte[] key, final byte[] min, final byte[] max) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZREMRANGEBYLEX, key, min, max);
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * Returns the score of member in the tairzset at key.
     * @param key
     * @param member
     * @return
     */
    public String exzscore(final String key, final String member) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZSCORE, key, member);
        return BuilderFactory.STRING.build(obj);
    }

    public byte[] exzscore(final byte[] key, final byte[] member) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZSCORE, key, member);
        return BuilderFactory.BYTE_ARRAY.build(obj);
    }

    /**
     * Returns the specified range of elements in the sorted set stored at key.
     * @param key
     * @param min
     * @param max
     * @return
     */
    public List<String> exzrange(final String key, final long min, final long max) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZRANGE, SafeEncoder.encode(key), toByteArray(min),
            toByteArray(max));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<byte[]> exzrange(final byte[] key, final long min, final long max) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZRANGE, key, toByteArray(min), toByteArray(max));
        return BuilderFactory.BYTE_ARRAY_LIST.build(obj);
    }

    public List<String> exzrangeWithScores(final String key, final long min, final long max) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZRANGE, SafeEncoder.encode(key), toByteArray(min),
            toByteArray(max), SafeEncoder.encode("WITHSCORES"));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<byte[]> exzrangeWithScores(final byte[] key, final long min, final long max) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZRANGE, key, toByteArray(min), toByteArray(max),
            SafeEncoder.encode("WITHSCORES"));
        return BuilderFactory.BYTE_ARRAY_LIST.build(obj);
    }

    /**
     * Returns the specified range of elements in the sorted set stored at key. The elements are considered to be
     * ordered from the highest to the lowest score.
     * @param key
     * @param min
     * @param max
     * @return
     */
    public List<String> exzrevrange(final String key, final long min, final long max) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZREVRANGE, SafeEncoder.encode(key), toByteArray(min),
            toByteArray(max));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<byte[]> exzrevrange(final byte[] key, final long min, final long max) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZREVRANGE, key, toByteArray(min), toByteArray(max));
        return BuilderFactory.BYTE_ARRAY_LIST.build(obj);
    }

    public List<String> exzrevrangeWithScores(final String key, final long min, final long max) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZREVRANGE, SafeEncoder.encode(key), toByteArray(min),
            toByteArray(max), SafeEncoder.encode("WITHSCORES"));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<byte[]> exzrevrangeWithScores(final byte[] key, final long min, final long max) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZREVRANGE, key, toByteArray(min), toByteArray(max),
            SafeEncoder.encode("WITHSCORES"));
        return BuilderFactory.BYTE_ARRAY_LIST.build(obj);
    }

    /**
     * Returns all the elements in the tairzset at key with a score between min and max (including elements with
     * score equal to min or max). The elements are considered to be ordered from low to high scores.
     * @param key
     * @param min
     * @param max
     * @return
     */
    public List<String> exzrangeByScore(final String key, final String min, final String max) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZRANGEBYSCORE, key, min, max);
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<byte[]> exzrangeByScore(final byte[] key, final byte[] min, final byte[] max) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZRANGEBYSCORE, key, min, max);
        return BuilderFactory.BYTE_ARRAY_LIST.build(obj);
    }

    public List<String> exzrangeByScore(final String key, final String min, final String max, final ExzrangeParams params) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZRANGEBYSCORE, params.getByteParams(SafeEncoder.encode(key),
            SafeEncoder.encode(min), SafeEncoder.encode(max)));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<byte[]> exzrangeByScore(final byte[] key, final byte[] min, final byte[] max, final ExzrangeParams params) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZRANGEBYSCORE, params.getByteParams(key, min, max));
        return BuilderFactory.BYTE_ARRAY_LIST.build(obj);
    }

    /**
     * Returns all the elements in the tairzset at key with a score between max and min (including elements with score
     * equal to max or min). In contrary to the default ordering of tairzsets, for this command the elements are
     * considered to be ordered from high to low scores.
     * @param key
     * @param min
     * @param max
     * @return
     */
    public List<String> exzrevrangeByScore(final String key, final String min, final String max) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZREVRANGEBYSCORE, key, min, max);
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<byte[]> exzrevrangeByScore(final byte[] key, final byte[] min, final byte[] max) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZREVRANGEBYSCORE, key, min, max);
        return BuilderFactory.BYTE_ARRAY_LIST.build(obj);
    }

    public List<String> exzrevrangeByScore(final String key, final String min, final String max, final ExzrangeParams params) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZREVRANGEBYSCORE, params.getByteParams(SafeEncoder.encode(key),
            SafeEncoder.encode(min), SafeEncoder.encode(max)));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<byte[]> exzrevrangeByScore(final byte[] key, final byte[] min, final byte[] max, final ExzrangeParams params) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZREVRANGEBYSCORE, params.getByteParams(key, min, max));
        return BuilderFactory.BYTE_ARRAY_LIST.build(obj);
    }

    /**
     *
     * @param key
     * @param min
     * @param max
     * @return
     */
    public List<String> exzrangeByLex(final String key, final String min, final String max) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZRANGEBYLEX, key, min, max);
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<byte[]> exzrangeByLex(final byte[] key, final byte[] min, final byte[] max) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZRANGEBYLEX, key, min, max);
        return BuilderFactory.BYTE_ARRAY_LIST.build(obj);
    }

    public List<String> exzrangeByLex(final String key, final String min, final String max, final ExzrangeParams params) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZRANGEBYLEX, params.getByteParams(SafeEncoder.encode(key),
            SafeEncoder.encode(min), SafeEncoder.encode(max)));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<byte[]> exzrangeByLex(final byte[] key, final byte[] min, final byte[] max, final ExzrangeParams params) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZRANGEBYLEX, params.getByteParams(key, min, max));
        return BuilderFactory.BYTE_ARRAY_LIST.build(obj);
    }

    /**
     *
     * @param key
     * @param min
     * @param max
     * @return
     */
    public List<String> exzrevrangeByLex(final String key, final String min, final String max) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZREVRANGEBYLEX, key, min, max);
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<byte[]> exzrevrangeByLex(final byte[] key, final byte[] min, final byte[] max) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZREVRANGEBYLEX, key, min, max);
        return BuilderFactory.BYTE_ARRAY_LIST.build(obj);
    }

    public List<String> exzrevrangeByLex(final String key, final String min, final String max, final ExzrangeParams params) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZREVRANGEBYLEX, params.getByteParams(SafeEncoder.encode(key),
            SafeEncoder.encode(min), SafeEncoder.encode(max)));
        return BuilderFactory.STRING_LIST.build(obj);
    }

    public List<byte[]> exzrevrangeByLex(final byte[] key, final byte[] min, final byte[] max, final ExzrangeParams params) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZREVRANGEBYLEX, params.getByteParams(key, min, max));
        return BuilderFactory.BYTE_ARRAY_LIST.build(obj);
    }

    /**
     * Returns the tairzset cardinality (number of elements) of the tairzset stored at key.
     * @param key
     * @return
     */
    public Long exzcard(final String key) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZCARD, key);
        return BuilderFactory.LONG.build(obj);
    }

    public Long exzcard(final byte[] key) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZCARD, key);
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * Returns the rank of member in the tairzset stored at key, with the scores ordered from low to high.
     * The rank (or index) is 0-based, which means that the member with the lowest score has rank 0.
     * @param key
     * @param member
     * @return
     */
    public Long exzrank(final String key, final String member) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZRANK, key, member);
        return BuilderFactory.LONG.build(obj);
    }

    public Long exzrank(final byte[] key, final byte[] member) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZRANK, key, member);
        return BuilderFactory.LONG.build(obj);
    }

    public Long exzrevrank(final String key, final String member) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZREVRANK, key, member);
        return BuilderFactory.LONG.build(obj);
    }

    public Long exzrevrank(final byte[] key, final byte[] member) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZREVRANK, key, member);
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * Same with zrank, but use score to get rank, when the field corresponding to score does not exist,
     * an estimate is used.
     * @param key
     * @param score
     * @return
     */
    public Long exzrankByScore(final String key, final String score) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZRANKBYSCORE, key, score);
        return BuilderFactory.LONG.build(obj);
    }

    public Long exzrankByScore(final byte[] key, final byte[] score) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZRANKBYSCORE, key, score);
        return BuilderFactory.LONG.build(obj);
    }

    public Long exzrevrankByScore(final String key, final String score) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZREVRANKBYSCORE, key, score);
        return BuilderFactory.LONG.build(obj);
    }

    public Long exzrevrankByScore(final byte[] key, final byte[] score) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZREVRANKBYSCORE, key, score);
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * Returns the number of elements in the tairzset at key with a score between min and max.
     * @param key
     * @param min
     * @param max
     * @return
     */
    public Long exzcount(final String key, final String min, final String max) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZCOUNT, key, min, max);
        return BuilderFactory.LONG.build(obj);
    }

    public Long exzcount(final byte[] key, final byte[] min, final byte[] max) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZCOUNT, key, min, max);
        return BuilderFactory.LONG.build(obj);
    }

    /**
     * When all the elements in a tairzset are inserted with the same score, in order to force lexicographical ordering,
     * this command returns the number of elements in the tairzset at key with a value between min and max.
     * @param key
     * @param min
     * @param max
     * @return
     */
    public Long exzlexcount(final String key, final String min, final String max) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZLEXCOUNT, key, min, max);
        return BuilderFactory.LONG.build(obj);
    }

    public Long exzlexcount(final byte[] key, final byte[] min, final byte[] max) {
        Object obj = getJedis().sendCommand(ModuleCommand.EXZLEXCOUNT, key, min, max);
        return BuilderFactory.LONG.build(obj);
    }
}
