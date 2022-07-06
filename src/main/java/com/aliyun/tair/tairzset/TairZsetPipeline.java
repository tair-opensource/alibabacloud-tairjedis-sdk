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
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.util.SafeEncoder;

import static com.aliyun.tair.tairzset.LeaderBoard.joinScoresToString;
import static redis.clients.jedis.Protocol.toByteArray;

public class TairZsetPipeline extends Pipeline {
    /**
     * Adds all the specified members with the specified (multi)scores to the tairzset stored at key.
     * @param key
     * @param member
     * @param scores
     * @return
     */
    public Response<Long> exzadd(final String key, final String member, final double... scores) {
        return exzadd(SafeEncoder.encode(key), SafeEncoder.encode(joinScoresToString(scores)),
            SafeEncoder.encode(member));
    }
    public Response<Long> exzadd(final byte[] key, final byte[] member, final double... scores) {
        return exzadd(key, SafeEncoder.encode(joinScoresToString(scores)), member);
    }

    public Response<Long> exzadd(final String key, final String score, final String member) {
        getClient("").sendCommand(ModuleCommand.EXZADD, key, score, member);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> exzadd(final byte[] key, final byte[] score, final byte[] member) {
        getClient("").sendCommand(ModuleCommand.EXZADD, key, score, member);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> exzadd(final String key, final String score, final String member, final ExzaddParams params) {
        return exzadd(SafeEncoder.encode(key), SafeEncoder.encode(score), SafeEncoder.encode(member), params);
    }

    public Response<Long> exzadd(final byte[] key, final byte[] score, final byte[] member, final ExzaddParams params) {
        getClient("").sendCommand(ModuleCommand.EXZADD, params.getByteParams(key, score, member));
        return getResponse(BuilderFactory.LONG);
    }

    @Deprecated
    public Response<Long> exzadd(final String key, final Map<String, String> scoreMembers) {
        final List<byte[]> bparams = new ArrayList<byte[]>();
        bparams.add(SafeEncoder.encode(key));

        for (final Entry<String, String> entry : scoreMembers.entrySet()) {
            bparams.add(SafeEncoder.encode(entry.getKey()));
            bparams.add(SafeEncoder.encode(entry.getValue()));
        }
        getClient("").sendCommand(ModuleCommand.EXZADD, bparams.toArray(new byte[bparams.size()][]));
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> exzaddMembers(final String key, final Map<String, String> members) {
        final List<byte[]> bparams = new ArrayList<byte[]>();
        bparams.add(SafeEncoder.encode(key));

        for (final Entry<String, String> entry : members.entrySet()) {
            bparams.add(SafeEncoder.encode(entry.getValue()));
            bparams.add(SafeEncoder.encode(entry.getKey()));
        }
        getClient("").sendCommand(ModuleCommand.EXZADD, bparams.toArray(new byte[bparams.size()][]));
        return getResponse(BuilderFactory.LONG);
    }

    @Deprecated
    public Response<Long> exzadd(final byte[] key, final Map<byte[], byte[]> scoreMembers) {
        final List<byte[]> bparams = new ArrayList<byte[]>();
        bparams.add(key);

        for (final Entry<byte[], byte[]> entry : scoreMembers.entrySet()) {
            bparams.add(entry.getKey());
            bparams.add(entry.getValue());
        }
        getClient("").sendCommand(ModuleCommand.EXZADD, bparams.toArray(new byte[bparams.size()][]));
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> exzaddMembers(final byte[] key, final Map<byte[], byte[]> members) {
        final List<byte[]> bparams = new ArrayList<byte[]>();
        bparams.add(key);

        for (final Entry<byte[], byte[]> entry : members.entrySet()) {
            bparams.add(entry.getValue());
            bparams.add(entry.getKey());
        }
        getClient("").sendCommand(ModuleCommand.EXZADD, bparams.toArray(new byte[bparams.size()][]));
        return getResponse(BuilderFactory.LONG);
    }

    @Deprecated
    public Response<Long> exzadd(final String key, final Map<String, String> scoreMembers, final ExzaddParams params) {
        final List<byte[]> bparams = new ArrayList<byte[]>();
        for (final Entry<String, String> entry : scoreMembers.entrySet()) {
            bparams.add(SafeEncoder.encode(entry.getKey()));
            bparams.add(SafeEncoder.encode(entry.getValue()));
        }
        getClient("").sendCommand(ModuleCommand.EXZADD,
            params.getByteParams(SafeEncoder.encode(key), bparams.toArray(new byte[bparams.size()][])));
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> exzaddMembers(final String key, final Map<String, String> members, final ExzaddParams params) {
        final List<byte[]> bparams = new ArrayList<byte[]>();
        for (final Entry<String, String> entry : members.entrySet()) {
            bparams.add(SafeEncoder.encode(entry.getValue()));
            bparams.add(SafeEncoder.encode(entry.getKey()));
        }
        getClient("").sendCommand(ModuleCommand.EXZADD,
            params.getByteParams(SafeEncoder.encode(key), bparams.toArray(new byte[bparams.size()][])));
        return getResponse(BuilderFactory.LONG);
    }

    @Deprecated
    public Response<Long> exzadd(final byte[] key, final Map<byte[], byte[]> scoreMembers, final ExzaddParams params) {
        final List<byte[]> bparams = new ArrayList<byte[]>();
        for (final Entry<byte[], byte[]> entry : scoreMembers.entrySet()) {
            bparams.add(entry.getKey());
            bparams.add(entry.getValue());
        }
        getClient("").sendCommand(ModuleCommand.EXZADD,
            params.getByteParams(key, bparams.toArray(new byte[bparams.size()][])));
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> exzaddMembers(final byte[] key, final Map<byte[], byte[]> members, final ExzaddParams params) {
        final List<byte[]> bparams = new ArrayList<byte[]>();
        for (final Entry<byte[], byte[]> entry : members.entrySet()) {
            bparams.add(entry.getValue());
            bparams.add(entry.getKey());
        }
        getClient("").sendCommand(ModuleCommand.EXZADD,
            params.getByteParams(key, bparams.toArray(new byte[bparams.size()][])));
        return getResponse(BuilderFactory.LONG);
    }

    /**
     * Increments the score of member in the tairzset stored at key by increment.
     * @param key
     * @param increment
     * @param member
     * @return
     */
    public Response<String> exzincrBy(final String key, final String increment, final String member) {
        getClient("").sendCommand(ModuleCommand.EXZINCRBY, key, increment, member);
        return getResponse(BuilderFactory.STRING);
    }

    public Response<byte[]> exzincrBy(final byte[] key, final byte[] increment, final byte[] member) {
        getClient("").sendCommand(ModuleCommand.EXZINCRBY, key, increment, member);
        return getResponse(BuilderFactory.BYTE_ARRAY);
    }

    public Response<String> exzincrBy(final String key, final String member, final double... scores) {
        getClient("").sendCommand(ModuleCommand.EXZINCRBY, key, joinScoresToString(scores), member);
        return getResponse(BuilderFactory.STRING);
    }

    public Response<byte[]> exzincrBy(final byte[] key, final byte[] member, final double... scores) {
        getClient("").sendCommand(ModuleCommand.EXZINCRBY, key,
            SafeEncoder.encode(joinScoresToString(scores)), member);
        return getResponse(BuilderFactory.BYTE_ARRAY);
    }

    /**
     * Removes the specified members from the tairzset stored at key. Non existing members are ignored.
     * @param key
     * @param member
     * @return
     */
    public Response<Long> exzrem(final String key, final String... member) {
        getClient("").sendCommand(ModuleCommand.EXZREM, JoinParameters.joinParameters(key, member));
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> exzrem(final byte[] key, final byte[]... member) {
        getClient("").sendCommand(ModuleCommand.EXZREM, JoinParameters.joinParameters(key, member));
        return getResponse(BuilderFactory.LONG);
    }

    /**
     * Removes all elements in the tairzset stored at key with a score between min and max (inclusive).
     * @param key
     * @param min
     * @param max
     * @return
     */
    public Response<Long> exzremrangeByScore(final String key, final String min, final String max) {
        getClient("").sendCommand(ModuleCommand.EXZREMRANGEBYSCORE, key, min, max);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> exzremrangeByScore(final byte[] key, final byte[] min, final byte[] max) {
        getClient("").sendCommand(ModuleCommand.EXZREMRANGEBYSCORE, key, min, max);
        return getResponse(BuilderFactory.LONG);
    }

    /**
     * Removes all elements in the tairzset stored at key with rank between start and stop.
     * @param key
     * @param start
     * @param stop
     * @return
     */
    public Response<Long> exzremrangeByRank(final String key, final long start, final long stop) {
        return exzremrangeByRank(SafeEncoder.encode(key), start, stop);
    }

    public Response<Long> exzremrangeByRank(final byte[] key, final long start, final long stop) {
        getClient("").sendCommand(ModuleCommand.EXZREMRANGEBYRANK, key, toByteArray(start),
            toByteArray(stop));
        return getResponse(BuilderFactory.LONG);
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
    public Response<Long> exzremrangeByLex(final String key, final String min, final String max) {
        getClient("").sendCommand(ModuleCommand.EXZREMRANGEBYLEX, key, min, max);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> exzremrangeByLex(final byte[] key, final byte[] min, final byte[] max) {
        getClient("").sendCommand(ModuleCommand.EXZREMRANGEBYLEX, key, min, max);
        return getResponse(BuilderFactory.LONG);
    }

    /**
     * Returns the score of member in the tairzset at key.
     * @param key
     * @param member
     * @return
     */
    public Response<String> exzscore(final String key, final String member) {
        getClient("").sendCommand(ModuleCommand.EXZSCORE, key, member);
        return getResponse(BuilderFactory.STRING);
    }

    public Response<byte[]> exzscore(final byte[] key, final byte[] member) {
        getClient("").sendCommand(ModuleCommand.EXZSCORE, key, member);
        return getResponse(BuilderFactory.BYTE_ARRAY);
    }

    /**
     * Returns the specified range of elements in the sorted set stored at key.
     * @param key
     * @param min
     * @param max
     * @return
     */
    public Response<List<String>> exzrange(final String key, final long min, final long max) {
        getClient("").sendCommand(ModuleCommand.EXZRANGE, SafeEncoder.encode(key), toByteArray(min),
            toByteArray(max));
        return getResponse(BuilderFactory.STRING_LIST);
    }

    public Response<List<byte[]>> exzrange(final byte[] key, final long min, final long max) {
        getClient("").sendCommand(ModuleCommand.EXZRANGE, key, toByteArray(min), toByteArray(max));
        return getResponse(BuilderFactory.BYTE_ARRAY_LIST);
    }

    public Response<List<String>> exzrangeWithScores(final String key, final long min, final long max) {
        getClient("").sendCommand(ModuleCommand.EXZRANGE, SafeEncoder.encode(key), toByteArray(min),
            toByteArray(max), SafeEncoder.encode("WITHSCORES"));
        return getResponse(BuilderFactory.STRING_LIST);
    }

    public Response<List<byte[]>> exzrangeWithScores(final byte[] key, final long min, final long max) {
        getClient("").sendCommand(ModuleCommand.EXZRANGE, key, toByteArray(min), toByteArray(max),
            SafeEncoder.encode("WITHSCORES"));
        return getResponse(BuilderFactory.BYTE_ARRAY_LIST);
    }

    /**
     * Returns the specified range of elements in the sorted set stored at key. The elements are considered to be
     * ordered from the highest to the lowest score.
     * @param key
     * @param min
     * @param max
     * @return
     */
    public Response<List<String>> exzrevrange(final String key, final long min, final long max) {
        getClient("").sendCommand(ModuleCommand.EXZREVRANGE, SafeEncoder.encode(key), toByteArray(min),
            toByteArray(max));
        return getResponse(BuilderFactory.STRING_LIST);
    }

    public Response<List<byte[]>> exzrevrange(final byte[] key, final long min, final long max) {
        getClient("").sendCommand(ModuleCommand.EXZREVRANGE, key, toByteArray(min), toByteArray(max));
        return getResponse(BuilderFactory.BYTE_ARRAY_LIST);
    }

    public Response<List<String>> exzrevrangeWithScores(final String key, final long min, final long max) {
        getClient("").sendCommand(ModuleCommand.EXZREVRANGE, SafeEncoder.encode(key), toByteArray(min),
            toByteArray(max), SafeEncoder.encode("WITHSCORES"));
        return getResponse(BuilderFactory.STRING_LIST);
    }

    public Response<List<byte[]>> exzrevrangeWithScores(final byte[] key, final long min, final long max) {
        getClient("").sendCommand(ModuleCommand.EXZREVRANGE, key, toByteArray(min), toByteArray(max),
            SafeEncoder.encode("WITHSCORES"));
        return getResponse(BuilderFactory.BYTE_ARRAY_LIST);
    }

    /**
     * Returns all the elements in the tairzset at key with a score between min and max (including elements with
     * score equal to min or max). The elements are considered to be ordered from low to high scores.
     * @param key
     * @param min
     * @param max
     * @return
     */
    public Response<List<String>> exzrangeByScore(final String key, final String min, final String max) {
        getClient("").sendCommand(ModuleCommand.EXZRANGEBYSCORE, key, min, max);
        return getResponse(BuilderFactory.STRING_LIST);
    }

    public Response<List<byte[]>> exzrangeByScore(final byte[] key, final byte[] min, final byte[] max) {
        getClient("").sendCommand(ModuleCommand.EXZRANGEBYSCORE, key, min, max);
        return getResponse(BuilderFactory.BYTE_ARRAY_LIST);
    }

    public Response<List<String>> exzrangeByScore(final String key, final String min, final String max, final ExzrangeParams params) {
        getClient("").sendCommand(ModuleCommand.EXZRANGEBYSCORE, params.getByteParams(SafeEncoder.encode(key),
            SafeEncoder.encode(min), SafeEncoder.encode(max)));
        return getResponse(BuilderFactory.STRING_LIST);
    }

    public Response<List<byte[]>> exzrangeByScore(final byte[] key, final byte[] min, final byte[] max, final ExzrangeParams params) {
        getClient("").sendCommand(ModuleCommand.EXZRANGEBYSCORE, params.getByteParams(key, min, max));
        return getResponse(BuilderFactory.BYTE_ARRAY_LIST);
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
    public Response<List<String>> exzrevrangeByScore(final String key, final String min, final String max) {
        getClient("").sendCommand(ModuleCommand.EXZREVRANGEBYSCORE, key, min, max);
        return getResponse(BuilderFactory.STRING_LIST);
    }

    public Response<List<byte[]>> exzrevrangeByScore(final byte[] key, final byte[] min, final byte[] max) {
        getClient("").sendCommand(ModuleCommand.EXZREVRANGEBYSCORE, key, min, max);
        return getResponse(BuilderFactory.BYTE_ARRAY_LIST);
    }

    public Response<List<String>> exzrevrangeByScore(final String key, final String min, final String max, final ExzrangeParams params) {
        getClient("").sendCommand(ModuleCommand.EXZREVRANGEBYSCORE, params.getByteParams(SafeEncoder.encode(key),
            SafeEncoder.encode(min), SafeEncoder.encode(max)));
        return getResponse(BuilderFactory.STRING_LIST);
    }

    public Response<List<byte[]>> exzrevrangeByScore(final byte[] key, final byte[] min, final byte[] max, final ExzrangeParams params) {
        getClient("").sendCommand(ModuleCommand.EXZREVRANGEBYSCORE, params.getByteParams(key, min, max));
        return getResponse(BuilderFactory.BYTE_ARRAY_LIST);
    }

    /**
     *
     * @param key
     * @param min
     * @param max
     * @return
     */
    public Response<List<String>> exzrangeByLex(final String key, final String min, final String max) {
        getClient("").sendCommand(ModuleCommand.EXZRANGEBYLEX, key, min, max);
        return getResponse(BuilderFactory.STRING_LIST);
    }

    public Response<List<byte[]>> exzrangeByLex(final byte[] key, final byte[] min, final byte[] max) {
        getClient("").sendCommand(ModuleCommand.EXZRANGEBYLEX, key, min, max);
        return getResponse(BuilderFactory.BYTE_ARRAY_LIST);
    }

    public Response<List<String>> exzrangeByLex(final String key, final String min, final String max, final ExzrangeParams params) {
        getClient("").sendCommand(ModuleCommand.EXZRANGEBYLEX, params.getByteParams(SafeEncoder.encode(key),
            SafeEncoder.encode(min), SafeEncoder.encode(max)));
        return getResponse(BuilderFactory.STRING_LIST);
    }

    public Response<List<byte[]>> exzrangeByLex(final byte[] key, final byte[] min, final byte[] max, final ExzrangeParams params) {
        getClient("").sendCommand(ModuleCommand.EXZRANGEBYLEX, params.getByteParams(key, min, max));
        return getResponse(BuilderFactory.BYTE_ARRAY_LIST);
    }

    /**
     *
     * @param key
     * @param min
     * @param max
     * @return
     */
    public Response<List<String>> exzrevrangeByLex(final String key, final String min, final String max) {
        getClient("").sendCommand(ModuleCommand.EXZREVRANGEBYLEX, key, min, max);
        return getResponse(BuilderFactory.STRING_LIST);
    }

    public Response<List<byte[]>> exzrevrangeByLex(final byte[] key, final byte[] min, final byte[] max) {
        getClient("").sendCommand(ModuleCommand.EXZREVRANGEBYLEX, key, min, max);
        return getResponse(BuilderFactory.BYTE_ARRAY_LIST);
    }

    public Response<List<String>> exzrevrangeByLex(final String key, final String min, final String max, final ExzrangeParams params) {
        getClient("").sendCommand(ModuleCommand.EXZREVRANGEBYLEX, params.getByteParams(SafeEncoder.encode(key),
            SafeEncoder.encode(min), SafeEncoder.encode(max)));
        return getResponse(BuilderFactory.STRING_LIST);
    }

    public Response<List<byte[]>> exzrevrangeByLex(final byte[] key, final byte[] min, final byte[] max, final ExzrangeParams params) {
        getClient("").sendCommand(ModuleCommand.EXZREVRANGEBYLEX, params.getByteParams(key, min, max));
        return getResponse(BuilderFactory.BYTE_ARRAY_LIST);
    }

    /**
     * Returns the tairzset cardinality (number of elements) of the tairzset stored at key.
     * @param key
     * @return
     */
    public Response<Long> exzcard(final String key) {
        getClient("").sendCommand(ModuleCommand.EXZCARD, key);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> exzcard(final byte[] key) {
        getClient("").sendCommand(ModuleCommand.EXZCARD, key);
        return getResponse(BuilderFactory.LONG);
    }

    /**
     * Returns the rank of member in the tairzset stored at key, with the scores ordered from low to high.
     * The rank (or index) is 0-based, which means that the member with the lowest score has rank 0.
     * @param key
     * @param member
     * @return
     */
    public Response<Long> exzrank(final String key, final String member) {
        getClient("").sendCommand(ModuleCommand.EXZRANK, key, member);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> exzrank(final byte[] key, final byte[] member) {
        getClient("").sendCommand(ModuleCommand.EXZRANK, key, member);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> exzrevrank(final String key, final String member) {
        getClient("").sendCommand(ModuleCommand.EXZREVRANK, key, member);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> exzrevrank(final byte[] key, final byte[] member) {
        getClient("").sendCommand(ModuleCommand.EXZREVRANK, key, member);
        return getResponse(BuilderFactory.LONG);
    }

    /**
     * Same with zrank, but use score to get rank, when the field corresponding to score does not exist,
     * an estimate is used.
     * @param key
     * @param score
     * @return
     */
    public Response<Long> exzrankByScore(final String key, final String score) {
        getClient("").sendCommand(ModuleCommand.EXZRANKBYSCORE, key, score);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> exzrankByScore(final byte[] key, final byte[] score) {
        getClient("").sendCommand(ModuleCommand.EXZRANKBYSCORE, key, score);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> exzrevrankByScore(final String key, final String score) {
        getClient("").sendCommand(ModuleCommand.EXZREVRANKBYSCORE, key, score);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> exzrevrankByScore(final byte[] key, final byte[] score) {
        getClient("").sendCommand(ModuleCommand.EXZREVRANKBYSCORE, key, score);
        return getResponse(BuilderFactory.LONG);
    }

    /**
     * Returns the number of elements in the tairzset at key with a score between min and max.
     * @param key
     * @param min
     * @param max
     * @return
     */
    public Response<Long> exzcount(final String key, final String min, final String max) {
        getClient("").sendCommand(ModuleCommand.EXZCOUNT, key, min, max);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> exzcount(final byte[] key, final byte[] min, final byte[] max) {
        getClient("").sendCommand(ModuleCommand.EXZCOUNT, key, min, max);
        return getResponse(BuilderFactory.LONG);
    }

    /**
     * When all the elements in a tairzset are inserted with the same score, in order to force lexicographical ordering,
     * this command returns the number of elements in the tairzset at key with a value between min and max.
     * @param key
     * @param min
     * @param max
     * @return
     */
    public Response<Long> exzlexcount(final String key, final String min, final String max) {
        getClient("").sendCommand(ModuleCommand.EXZLEXCOUNT, key, min, max);
        return getResponse(BuilderFactory.LONG);
    }

    public Response<Long> exzlexcount(final byte[] key, final byte[] min, final byte[] max) {
        getClient("").sendCommand(ModuleCommand.EXZLEXCOUNT, key, min, max);
        return getResponse(BuilderFactory.LONG);
    }
}
