package com.aliyun.tair.tairzset;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.jedis3.Jedis3BuilderFactory;
import com.aliyun.tair.tairzset.params.ExzaddParams;
import com.aliyun.tair.tairzset.params.ExzrangeParams;
import com.aliyun.tair.util.JoinParameters;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.CommandArguments;
import redis.clients.jedis.CommandObject;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.util.SafeEncoder;

import static com.aliyun.tair.tairzset.LeaderBoard.joinScoresToString;
import static redis.clients.jedis.Protocol.toByteArray;

public class TairZsetPipeline extends Pipeline {
    public TairZsetPipeline(Jedis jedis) {
        super(jedis);
    }

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
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZADD)
            .add(key)
            .add(score)
            .add(member), BuilderFactory.LONG));
    }

    public Response<Long> exzadd(final byte[] key, final byte[] score, final byte[] member) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZADD)
            .add(key)
            .add(score)
            .add(member), BuilderFactory.LONG));
    }

    public Response<Long> exzadd(final String key, final String score, final String member, final ExzaddParams params) {
        return exzadd(SafeEncoder.encode(key), SafeEncoder.encode(score), SafeEncoder.encode(member), params);
    }

    public Response<Long> exzadd(final byte[] key, final byte[] score, final byte[] member, final ExzaddParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZADD)
            .addObjects(params.getByteParams(key, score, member)), BuilderFactory.LONG));
    }

    @Deprecated
    public Response<Long> exzadd(final String key, final Map<String, String> scoreMembers) {
        final List<byte[]> bparams = new ArrayList<byte[]>();
        bparams.add(SafeEncoder.encode(key));
        for (final Entry<String, String> entry : scoreMembers.entrySet()) {
            bparams.add(SafeEncoder.encode(entry.getKey()));
            bparams.add(SafeEncoder.encode(entry.getValue()));
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZADD)
            .addObjects(bparams), BuilderFactory.LONG));
    }

    public Response<Long> exzaddMembers(final String key, final Map<String, String> members) {
        final List<byte[]> bparams = new ArrayList<byte[]>();
        bparams.add(SafeEncoder.encode(key));
        for (final Entry<String, String> entry : members.entrySet()) {
            bparams.add(SafeEncoder.encode(entry.getValue()));
            bparams.add(SafeEncoder.encode(entry.getKey()));
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZADD)
            .addObjects(bparams), BuilderFactory.LONG));
    }

    @Deprecated
    public Response<Long> exzadd(final byte[] key, final Map<byte[], byte[]> scoreMembers) {
        final List<byte[]> bparams = new ArrayList<byte[]>();
        bparams.add(key);
        for (final Entry<byte[], byte[]> entry : scoreMembers.entrySet()) {
            bparams.add(entry.getKey());
            bparams.add(entry.getValue());
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZADD)
            .addObjects(bparams), BuilderFactory.LONG));
    }

    public Response<Long> exzaddMembers(final byte[] key, final Map<byte[], byte[]> members) {
        final List<byte[]> bparams = new ArrayList<byte[]>();
        bparams.add(key);
        for (final Entry<byte[], byte[]> entry : members.entrySet()) {
            bparams.add(entry.getValue());
            bparams.add(entry.getKey());
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZADD)
            .addObjects(bparams), BuilderFactory.LONG));
    }

    @Deprecated
    public Response<Long> exzadd(final String key, final Map<String, String> scoreMembers, final ExzaddParams params) {
        final List<byte[]> bparams = new ArrayList<byte[]>();
        for (final Entry<String, String> entry : scoreMembers.entrySet()) {
            bparams.add(SafeEncoder.encode(entry.getKey()));
            bparams.add(SafeEncoder.encode(entry.getValue()));
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZADD)
            .addObjects(params.getByteParams(SafeEncoder.encode(key), bparams.toArray(new byte[bparams.size()][]))), BuilderFactory.LONG));
    }

    public Response<Long> exzaddMembers(final String key, final Map<String, String> members, final ExzaddParams params) {
        final List<byte[]> bparams = new ArrayList<byte[]>();
        for (final Entry<String, String> entry : members.entrySet()) {
            bparams.add(SafeEncoder.encode(entry.getValue()));
            bparams.add(SafeEncoder.encode(entry.getKey()));
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZADD)
            .addObjects(params.getByteParams(SafeEncoder.encode(key), bparams.toArray(new byte[bparams.size()][]))), BuilderFactory.LONG));
    }

    @Deprecated
    public Response<Long> exzadd(final byte[] key, final Map<byte[], byte[]> scoreMembers, final ExzaddParams params) {
        final List<byte[]> bparams = new ArrayList<byte[]>();
        for (final Entry<byte[], byte[]> entry : scoreMembers.entrySet()) {
            bparams.add(entry.getKey());
            bparams.add(entry.getValue());
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZADD)
            .addObjects(params.getByteParams(key, bparams.toArray(new byte[bparams.size()][]))), BuilderFactory.LONG));
    }

    public Response<Long> exzaddMembers(final byte[] key, final Map<byte[], byte[]> members, final ExzaddParams params) {
        final List<byte[]> bparams = new ArrayList<byte[]>();
        for (final Entry<byte[], byte[]> entry : members.entrySet()) {
            bparams.add(entry.getValue());
            bparams.add(entry.getKey());
        }
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZADD)
            .addObjects(params.getByteParams(key, bparams.toArray(new byte[bparams.size()][]))), BuilderFactory.LONG));
    }

    /**
     * Increments the score of member in the tairzset stored at key by increment.
     * @param key
     * @param increment
     * @param member
     * @return
     */
    public Response<String> exzincrBy(final String key, final String increment, final String member) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZINCRBY)
            .add(key)
            .add(increment)
            .add(member), BuilderFactory.STRING));
    }

    public Response<byte[]> exzincrBy(final byte[] key, final byte[] increment, final byte[] member) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZINCRBY)
            .add(key)
            .add(increment)
            .add(member), Jedis3BuilderFactory.BYTE_ARRAY));
    }

    public Response<String> exzincrBy(final String key, final String member, final double... scores) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZINCRBY)
            .add(key)
            .add(joinScoresToString(scores))
            .add(member), BuilderFactory.STRING));
    }

    public Response<byte[]> exzincrBy(final byte[] key, final byte[] member, final double... scores) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZINCRBY)
            .add(key)
            .add(SafeEncoder.encode(joinScoresToString(scores)))
            .add(member), Jedis3BuilderFactory.BYTE_ARRAY));
    }

    /**
     * Removes the specified members from the tairzset stored at key. Non existing members are ignored.
     * @param key
     * @param member
     * @return
     */
    public Response<Long> exzrem(final String key, final String... member) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZREM)
            .addObjects(JoinParameters.joinParameters(key, member)), BuilderFactory.LONG));
    }

    public Response<Long> exzrem(final byte[] key, final byte[]... member) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZREM)
            .addObjects(JoinParameters.joinParameters(key, member)), BuilderFactory.LONG));
    }

    /**
     * Removes all elements in the tairzset stored at key with a score between min and max (inclusive).
     * @param key
     * @param min
     * @param max
     * @return
     */
    public Response<Long> exzremrangeByScore(final String key, final String min, final String max) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZREMRANGEBYSCORE)
            .add(key)
            .add(min)
            .add(max), BuilderFactory.LONG));
    }

    public Response<Long> exzremrangeByScore(final byte[] key, final byte[] min, final byte[] max) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZREMRANGEBYSCORE)
            .add(key)
            .add(min)
            .add(max), BuilderFactory.LONG));
    }

    /**
     * Removes all elements in the tairzset stored at key with rank between start and stop.
     * @param key
     * @param start
     * @param stop
     * @return
     */
    public Response<Long> exzremrangeByRank(final String key, final long start, final long stop) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZREMRANGEBYRANK)
            .add(key)
            .add(toByteArray(start))
            .add(toByteArray(stop)), BuilderFactory.LONG));
    }

    public Response<Long> exzremrangeByRank(final byte[] key, final long start, final long stop) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZREMRANGEBYRANK)
            .add(key)
            .add(toByteArray(start))
            .add(toByteArray(stop)), BuilderFactory.LONG));
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
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZREMRANGEBYLEX)
            .add(key)
            .add(min)
            .add(max), BuilderFactory.LONG));
    }

    public Response<Long> exzremrangeByLex(final byte[] key, final byte[] min, final byte[] max) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZREMRANGEBYLEX)
            .add(key)
            .add(min)
            .add(max), BuilderFactory.LONG));
    }

    /**
     * Returns the score of member in the tairzset at key.
     * @param key
     * @param member
     * @return
     */
    public Response<String> exzscore(final String key, final String member) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZSCORE)
            .add(key)
            .add(member), BuilderFactory.STRING));
    }

    public Response<byte[]> exzscore(final byte[] key, final byte[] member) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZSCORE)
            .add(key)
            .add(member), Jedis3BuilderFactory.BYTE_ARRAY));
    }

    /**
     * Returns the specified range of elements in the sorted set stored at key.
     * @param key
     * @param min
     * @param max
     * @return
     */
    public Response<List<String>> exzrange(final String key, final long min, final long max) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZRANGE)
            .add(SafeEncoder.encode(key))
            .add(toByteArray(min))
            .add(toByteArray(max)), BuilderFactory.STRING_LIST));
    }

    public Response<List<byte[]>> exzrange(final byte[] key, final long min, final long max) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZRANGE)
            .add(key)
            .add(toByteArray(min))
            .add(toByteArray(max)), Jedis3BuilderFactory.BYTE_ARRAY_LIST));
    }

    public Response<List<String>> exzrangeWithScores(final String key, final long min, final long max) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZRANGE)
            .add(SafeEncoder.encode(key))
            .add(toByteArray(min))
            .add(toByteArray(max))
            .add(SafeEncoder.encode("WITHSCORES")), BuilderFactory.STRING_LIST));
    }

    public Response<List<byte[]>> exzrangeWithScores(final byte[] key, final long min, final long max) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZRANGE)
            .add(key)
            .add(toByteArray(min))
            .add(toByteArray(max))
            .add(SafeEncoder.encode("WITHSCORES")), Jedis3BuilderFactory.BYTE_ARRAY_LIST));
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
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZREVRANGE)
            .add(SafeEncoder.encode(key))
            .add(toByteArray(min))
            .add(toByteArray(max)), BuilderFactory.STRING_LIST));
    }

    public Response<List<byte[]>> exzrevrange(final byte[] key, final long min, final long max) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZREVRANGE)
            .add(key)
            .add(toByteArray(min))
            .add(toByteArray(max)), Jedis3BuilderFactory.BYTE_ARRAY_LIST));
    }

    public Response<List<String>> exzrevrangeWithScores(final String key, final long min, final long max) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZREVRANGE)
            .add(SafeEncoder.encode(key))
            .add(toByteArray(min))
            .add(toByteArray(max))
            .add(SafeEncoder.encode("WITHSCORES")), BuilderFactory.STRING_LIST));
    }

    public Response<List<byte[]>> exzrevrangeWithScores(final byte[] key, final long min, final long max) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZREVRANGE)
            .add(key)
            .add(toByteArray(min))
            .add(toByteArray(max))
            .add(SafeEncoder.encode("WITHSCORES")), Jedis3BuilderFactory.BYTE_ARRAY_LIST));
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
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZRANGEBYSCORE)
            .add(key)
            .add(min)
            .add(max), BuilderFactory.STRING_LIST));
    }

    public Response<List<byte[]>> exzrangeByScore(final byte[] key, final byte[] min, final byte[] max) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZRANGEBYSCORE)
            .add(key)
            .add(min)
            .add(max), Jedis3BuilderFactory.BYTE_ARRAY_LIST));
    }

    public Response<List<String>> exzrangeByScore(final String key, final String min, final String max, final ExzrangeParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZRANGEBYSCORE)
            .addObjects(params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(min), SafeEncoder.encode(max))), BuilderFactory.STRING_LIST));
    }

    public Response<List<byte[]>> exzrangeByScore(final byte[] key, final byte[] min, final byte[] max, final ExzrangeParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZRANGEBYSCORE)
            .addObjects(params.getByteParams(key, min, max)), Jedis3BuilderFactory.BYTE_ARRAY_LIST));
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
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZREVRANGEBYSCORE)
            .add(key)
            .add(min)
            .add(max), BuilderFactory.STRING_LIST));
    }

    public Response<List<byte[]>> exzrevrangeByScore(final byte[] key, final byte[] min, final byte[] max) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZREVRANGEBYSCORE)
            .add(key)
            .add(min)
            .add(max), Jedis3BuilderFactory.BYTE_ARRAY_LIST));
    }

    public Response<List<String>> exzrevrangeByScore(final String key, final String min, final String max, final ExzrangeParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZREVRANGEBYSCORE)
            .addObjects(params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(min), SafeEncoder.encode(max))), BuilderFactory.STRING_LIST));
    }

    public Response<List<byte[]>> exzrevrangeByScore(final byte[] key, final byte[] min, final byte[] max, final ExzrangeParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZREVRANGEBYSCORE)
            .addObjects(params.getByteParams(key, min, max)), Jedis3BuilderFactory.BYTE_ARRAY_LIST));
    }

    /**
     *
     * @param key
     * @param min
     * @param max
     * @return
     */
    public Response<List<String>> exzrangeByLex(final String key, final String min, final String max) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZRANGEBYLEX)
            .add(key)
            .add(min)
            .add(max), BuilderFactory.STRING_LIST));
    }

    public Response<List<byte[]>> exzrangeByLex(final byte[] key, final byte[] min, final byte[] max) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZRANGEBYLEX)
            .add(key)
            .add(min)
            .add(max), Jedis3BuilderFactory.BYTE_ARRAY_LIST));
    }

    public Response<List<String>> exzrangeByLex(final String key, final String min, final String max, final ExzrangeParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZRANGEBYLEX)
            .addObjects(params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(min), SafeEncoder.encode(max))), BuilderFactory.STRING_LIST));
    }

    public Response<List<byte[]>> exzrangeByLex(final byte[] key, final byte[] min, final byte[] max, final ExzrangeParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZRANGEBYLEX)
            .addObjects(params.getByteParams(key, min, max)), Jedis3BuilderFactory.BYTE_ARRAY_LIST));
    }

    /**
     *
     * @param key
     * @param min
     * @param max
     * @return
     */
    public Response<List<String>> exzrevrangeByLex(final String key, final String min, final String max) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZREVRANGEBYLEX)
            .add(key)
            .add(min)
            .add(max), BuilderFactory.STRING_LIST));
    }

    public Response<List<byte[]>> exzrevrangeByLex(final byte[] key, final byte[] min, final byte[] max) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZREVRANGEBYLEX)
            .add(key)
            .add(min)
            .add(max), Jedis3BuilderFactory.BYTE_ARRAY_LIST));
    }

    public Response<List<String>> exzrevrangeByLex(final String key, final String min, final String max, final ExzrangeParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZREVRANGEBYLEX)
            .addObjects(params.getByteParams(SafeEncoder.encode(key), SafeEncoder.encode(min), SafeEncoder.encode(max))), BuilderFactory.STRING_LIST));
    }

    public Response<List<byte[]>> exzrevrangeByLex(final byte[] key, final byte[] min, final byte[] max, final ExzrangeParams params) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZREVRANGEBYLEX)
            .addObjects(params.getByteParams(key, min, max)), Jedis3BuilderFactory.BYTE_ARRAY_LIST));
    }

    /**
     * Returns the tairzset cardinality (number of elements) of the tairzset stored at key.
     * @param key
     * @return
     */
    public Response<Long> exzcard(final String key) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZCARD)
            .add(key), BuilderFactory.LONG));
    }

    public Response<Long> exzcard(final byte[] key) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZCARD)
            .add(key), BuilderFactory.LONG));
    }

    /**
     * Returns the rank of member in the tairzset stored at key, with the scores ordered from low to high.
     * The rank (or index) is 0-based, which means that the member with the lowest score has rank 0.
     * @param key
     * @param member
     * @return
     */
    public Response<Long> exzrank(final String key, final String member) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZRANK)
            .add(key)
            .add(member), BuilderFactory.LONG));
    }

    public Response<Long> exzrank(final byte[] key, final byte[] member) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZRANK)
            .add(key)
            .add(member), BuilderFactory.LONG));
    }

    public Response<Long> exzrevrank(final String key, final String member) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZREVRANK)
            .add(key)
            .add(member), BuilderFactory.LONG));
    }

    public Response<Long> exzrevrank(final byte[] key, final byte[] member) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZREVRANK)
            .add(key)
            .add(member), BuilderFactory.LONG));
    }

    /**
     * Same with zrank, but use score to get rank, when the field corresponding to score does not exist,
     * an estimate is used.
     * @param key
     * @param score
     * @return
     */
    public Response<Long> exzrankByScore(final String key, final String score) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZRANKBYSCORE)
            .add(key)
            .add(score), BuilderFactory.LONG));
    }

    public Response<Long> exzrankByScore(final byte[] key, final byte[] score) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZRANKBYSCORE)
            .add(key)
            .add(score), BuilderFactory.LONG));
    }

    public Response<Long> exzrevrankByScore(final String key, final String score) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZREVRANKBYSCORE)
            .add(key)
            .add(score), BuilderFactory.LONG));
    }

    public Response<Long> exzrevrankByScore(final byte[] key, final byte[] score) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZREVRANKBYSCORE)
            .add(key)
            .add(score), BuilderFactory.LONG));
    }

    /**
     * Returns the number of elements in the tairzset at key with a score between min and max.
     * @param key
     * @param min
     * @param max
     * @return
     */
    public Response<Long> exzcount(final String key, final String min, final String max) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZCOUNT)
            .add(key)
            .add(min)
            .add(max), BuilderFactory.LONG));
    }

    public Response<Long> exzcount(final byte[] key, final byte[] min, final byte[] max) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZCOUNT)
            .add(key)
            .add(min)
            .add(max), BuilderFactory.LONG));
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
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZLEXCOUNT)
            .add(key)
            .add(min)
            .add(max), BuilderFactory.LONG));
    }

    public Response<Long> exzlexcount(final byte[] key, final byte[] min, final byte[] max) {
        return appendCommand(new CommandObject<>(new CommandArguments(ModuleCommand.EXZLEXCOUNT)
            .add(key)
            .add(min)
            .add(max), BuilderFactory.LONG));
    }
}
