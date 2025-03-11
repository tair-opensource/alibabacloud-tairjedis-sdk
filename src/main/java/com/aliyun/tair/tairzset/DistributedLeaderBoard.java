package com.aliyun.tair.tairzset;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.tairzset.params.RankParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.valkey.BuilderFactory;
import io.valkey.Jedis;
import io.valkey.JedisPool;
import io.valkey.Pipeline;
import io.valkey.Protocol.Command;
import io.valkey.util.JedisClusterCRC16;
import io.valkey.util.SafeEncoder;

import static com.aliyun.tair.tairzset.LeaderBoard.joinScoresToString;
import static io.valkey.Protocol.toByteArray;

public class DistributedLeaderBoard {
    private static final Logger LOGGER = LoggerFactory.getLogger(DistributedLeaderBoard.class);

    private static final String CH = "CH";
    private static final String WITHSCORES = "WITHSCORES";
    private static final int DEFAULT_PAGE_SIZE = 10;
    private static final int DEFAULT_SHARDKEY_SIZE = 10;
    private static final boolean DEFAULT_REVERSE = false;
    private static final boolean DEFAULT_USE_ZERO_INDEX = true;
    public static final boolean DEFAULT_QUERY_RANK_FROM_REDIS = false;

    private final String name;
    private final byte[] nameBinary;
    private final JedisPool jedisPool;
    private final int shardKeySize;
    private final int pageSize;
    private final boolean reverse;
    private final boolean useZeroIndexForRank;
    private final boolean queryRankFromRedis;

    public DistributedLeaderBoard(String name, JedisPool jedisPool) {
        this(name, jedisPool, DEFAULT_SHARDKEY_SIZE);
    }

    public DistributedLeaderBoard(String name, JedisPool jedisPool, int shardKeySize) {
        this(name, jedisPool, shardKeySize, DEFAULT_PAGE_SIZE);
    }

    public DistributedLeaderBoard(String name, JedisPool jedisPool, int shardKeySize, int pageSize) {
        this(name, jedisPool, shardKeySize, pageSize, DEFAULT_REVERSE);
    }

    public DistributedLeaderBoard(String name, JedisPool jedisPool, int shardKeySize, int pageSize, boolean reverse) {
        this(name, jedisPool, shardKeySize, pageSize, reverse, DEFAULT_USE_ZERO_INDEX);
    }

    public DistributedLeaderBoard(String name, JedisPool jedisPool, int shardKeySize, int pageSize, boolean reverse,
        boolean useZeroIndexForRank) {
        this(name, jedisPool, shardKeySize, pageSize, reverse, useZeroIndexForRank, DEFAULT_QUERY_RANK_FROM_REDIS);
    }

    public DistributedLeaderBoard(String name, JedisPool jedisPool, int shardKeySize, int pageSize, boolean reverse,
        boolean useZeroIndexForRank, boolean queryRankFromRedis) {
        this.name = name;
        this.nameBinary = SafeEncoder.encode(name);
        this.jedisPool = jedisPool;
        this.shardKeySize = shardKeySize;
        this.pageSize = pageSize;
        this.reverse = reverse;
        this.useZeroIndexForRank = useZeroIndexForRank;
        this.queryRankFromRedis = queryRankFromRedis;
    }

    private String crcKeyByMember(final String member) {
        int index = JedisClusterCRC16.getSlot(member) % shardKeySize;
        return name + "_" + index;
    }

    private String crcKeyByMember(final byte[] member) {
        int index = JedisClusterCRC16.getSlot(member) % shardKeySize;
        return name + "_" + index;
    }

    private String joinKeyAndIndex(final int index) {
        return name + "_" + index;
    }

    /**
     * Add a member to leaderboard.
     *
     * @param member the member
     * @param scores the score
     * @return true: success set or update, false: element already exists and has the same score.
     */
    public Boolean addMember(final String member, final String scores) {
        return addMember(SafeEncoder.encode(member), SafeEncoder.encode(scores));
    }

    public Boolean addMember(final String member, final double... scores) {
        return addMember(SafeEncoder.encode(member), SafeEncoder.encode(joinScoresToString(scores)));
    }

    public Boolean addMember(final byte[] member, final double... scores) {
        return addMember(member, SafeEncoder.encode(joinScoresToString(scores)));
    }

    public Boolean addMember(final byte[] member, final byte[] score) {
        try (Jedis jedis = jedisPool.getResource()) {
            Object obj = jedis.sendCommand(ModuleCommand.EXZADD, SafeEncoder.encode(crcKeyByMember(member)),
                SafeEncoder.encode(CH), score, member);
            return BuilderFactory.BOOLEAN.build(obj);
        }
    }

    /**
     * Add multi members to leaderboard.
     * @param memberScores key : member, value: score
     * @return list of insert status.
     */
    public List<Boolean> addMember(final Map<String, String> memberScores) {
        List<Boolean> results = new ArrayList<>();
        try (Jedis jedis = jedisPool.getResource()) {
            Pipeline p = jedis.pipelined();
            for (Map.Entry<String, String> entry : memberScores.entrySet()) {
                p.sendCommand(ModuleCommand.EXZADD, SafeEncoder.encode(crcKeyByMember(entry.getKey())),
                    SafeEncoder.encode(CH), SafeEncoder.encode(entry.getValue()), SafeEncoder.encode(entry.getKey()));
            }

            List<Object> objs = p.syncAndReturnAll();
            for (Object obj : objs) {
                results.add(BuilderFactory.BOOLEAN.build(obj));
            }
        }
        return results;
    }

    /**
     * Change score for member.
     *
     * @param member the member
     * @param increment the increment (negative value to decrement)
     * @return the new score of member.
     */
    public String incrScoreFor(final String member, final String increment) {
        return incrScoreFor(SafeEncoder.encode(member), SafeEncoder.encode(increment));
    }

    public String incrScoreFor(final byte[] member, final byte[] increment) {
        try (Jedis jedis = jedisPool.getResource()) {
            Object obj = jedis.sendCommand(ModuleCommand.EXZINCRBY, SafeEncoder.encode(crcKeyByMember(member)),
                increment, member);
            return BuilderFactory.STRING.build(obj);
        }
    }

    /**
     * Remove member from leaderboard.
     *
     * @param member the member
     * @return The number of members removed from the leaderboard.
     */
    public Long removeMember(final String member) {
        return removeMember(SafeEncoder.encode(member));
    }

    public Long removeMember(final byte[] member) {
        try (Jedis jedis = jedisPool.getResource()) {
            Object obj = jedis.sendCommand(ModuleCommand.EXZREM, SafeEncoder.encode(crcKeyByMember(member)), member);
            return BuilderFactory.LONG.build(obj);
        }
    }

    /**
     * Retrieve member by offset from leaderboard. The interval is [startOffset, endOffset]
     *
     * @param startOffset
     * @param endOffset
     * @return
     */
    public List<LeaderData> retrieveMember(final long startOffset, final long endOffset) {
        List<LeaderData> leaderDataList = top(endOffset + 1);
        return leaderDataList.subList((int)startOffset, (int)endOffset + 1);
    }

    /**
     * Total members of the leaderboard.
     *
     * @return total members of the leaderboard, 0 if key does not exist.
     */
    public Long totalMembers() {
        long rank = 0;
        try (Jedis jedis = jedisPool.getResource()) {
            Pipeline p = jedis.pipelined();
            for (int i = 0; i < shardKeySize; i++) {
                p.sendCommand(ModuleCommand.EXZCARD, SafeEncoder.encode(joinKeyAndIndex(i)));
            }

            List<Object> objs = p.syncAndReturnAll();
            for (int i = 0; i < objs.size(); i++) {
                if (objs.get(i) == null) {
                    LOGGER.error("Could not find key: {}", SafeEncoder.encode(joinKeyAndIndex(i)));
                    continue;
                }
                rank += BuilderFactory.LONG.build(objs.get(i));
            }
        }
        return rank;
    }

    /**
     * Total pages of the leaderboard.
     *
     * @return total pages of the leaderboard.
     */
    public Long totalPages() {
        return (long)Math.ceil((double)totalMembers() / (double)pageSize);
    }

    /**
     * The total of members in the current leaderboard in a score range.
     *
     * @param minScore Minimum score
     * @param maxScore Maximum score
     * @return Total of members in the current leaderboard in a score range.
     */
    public Long totalMembersInScoreRange(final double minScore, final double maxScore) {
        String minScoreStr = joinScoresToString(minScore);
        String maxScoreStr = joinScoresToString(maxScore);
        return totalMembersInScoreRange(minScoreStr, maxScoreStr);
    }

    public Long totalMembersInScoreRange(final String minScore, final String maxScore) {
        long counts = 0;
        try (Jedis jedis = jedisPool.getResource()) {
            Pipeline p = jedis.pipelined();
            for (int i = 0; i < shardKeySize; i++) {
                p.sendCommand(ModuleCommand.EXZCOUNT, SafeEncoder.encode(joinKeyAndIndex(i)),
                    SafeEncoder.encode(minScore), SafeEncoder.encode(maxScore));
            }

            List<Object> objs = p.syncAndReturnAll();
            for (int i = 0; i < objs.size(); i++) {
                counts += BuilderFactory.LONG.build(objs.get(i));
            }
        }
        return counts;
    }

    /**
     * Remove members from the current leaderboard in a given score range.
     *
     * @param minScore Minimum score
     * @param maxScore Maximum score
     * @return the number of members removed.
     */
    public Long removeMembersInScoreRange(final double minScore, final double maxScore) {
        String minScoreStr = joinScoresToString(minScore);
        String maxScoreStr = joinScoresToString(maxScore);
        return removeMembersInScoreRange(minScoreStr, maxScoreStr);
    }

    public Long removeMembersInScoreRange(final String minScore, final String maxScore) {
        long counts = 0;
        try (Jedis jedis = jedisPool.getResource()) {
            Pipeline p = jedis.pipelined();
            for (int i = 0; i < shardKeySize; i++) {
                p.sendCommand(ModuleCommand.EXZREMRANGEBYSCORE, SafeEncoder.encode(joinKeyAndIndex(i)),
                    SafeEncoder.encode(minScore), SafeEncoder.encode(maxScore));
            }

            List<Object> objs = p.syncAndReturnAll();
            for (int i = 0; i < objs.size(); i++) {
                counts += BuilderFactory.LONG.build(objs.get(i));
            }
        }
        return counts;
    }

    /**
     * Retrieve the score for a member in the current leaderboard.
     *
     * @param member Member
     * @return Member score.
     */
    public String scoreFor(final String member) {
        return scoreFor(SafeEncoder.encode(member));
    }

    public String scoreFor(final byte[] member) {
        try (Jedis jedis = jedisPool.getResource()) {
            Object obj = jedis.sendCommand(ModuleCommand.EXZSCORE, SafeEncoder.encode(crcKeyByMember(member)), member);
            return BuilderFactory.STRING.build(obj);
        }
    }

    /**
     * Retrieve the rank for a member in the current leaderboard.
     *
     * @param member Member
     * @return Rank for member in the current leaderboard.
     */
    public Long rankFor(final String member) {
        return rankFor(SafeEncoder.encode(member), new RankParams());
    }

    public Long rankFor(final byte[] member) {
        return rankFor(member, new RankParams());
    }

    public Long rankFor(final String member, final RankParams rankParams) {
        return rankFor(SafeEncoder.encode(member), rankParams);
    }

    public Long rankFor(final byte[] member, final RankParams rankParams) {
        long rank = 0;
        String score = scoreFor(member);
        if (score == null) {
            return -1L;
        }

        try (Jedis jedis = jedisPool.getResource()) {
            Pipeline p = jedis.pipelined();
            for (int i = 0; i < shardKeySize; i++) {
                if (reverse) {
                    p.sendCommand(ModuleCommand.EXZREVRANKBYSCORE,
                        rankParams.getByteParams(SafeEncoder.encode(joinKeyAndIndex(i)), SafeEncoder.encode(score)));
                } else {
                    p.sendCommand(ModuleCommand.EXZRANKBYSCORE,
                        rankParams.getByteParams(SafeEncoder.encode(joinKeyAndIndex(i)), SafeEncoder.encode(score)));
                }
            }

            List<Object> objs = p.syncAndReturnAll();
            for (int i = 0; i < objs.size(); i++) {
                if (objs.get(i) == null) {
                    LOGGER.error("Could not find key: {}", SafeEncoder.encode(joinKeyAndIndex(i)));
                    continue;
                }
                rank += BuilderFactory.LONG.build(objs.get(i));
            }
        }

        if (!useZeroIndexForRank) {
            rank += 1;
        }
        if (reverse) {
            rank -= 1;
        }
        return rank;
    }

    /**
     * Retrieve score and rank for a member in the current leaderboard.
     *
     * @param member Member
     * @return Score and rank for a member in the current leaderboard.
     */
    public LeaderData scoreAndRankFor(final String member) {
        return scoreAndRankFor(SafeEncoder.encode(member));
    }

    public LeaderData scoreAndRankFor(final byte[] member) {
        String score = scoreFor(member);
        Long rank = rankFor(member);
        return new LeaderData(new String(member), score, rank);
    }

    /**
     * Get the leaderboard top number.
     *
     * @param number the number
     * @return members from the leaderboard that fall within the given rank range.
     */
    public List<LeaderData> top(long number) {
        List<LeaderData> leaderDataList = new ArrayList<>();

        if (number < 1) {
            number = 1;
        }
        long totalMembers = totalMembers();
        long startRank = 0;
        long endRank = number > totalMembers ? totalMembers-1 : number-1;

        try (Jedis jedis = jedisPool.getResource()) {
            Pipeline p = jedis.pipelined();
            for (int i = 0; i < shardKeySize; i++) {
                if (reverse) {
                    p.sendCommand(ModuleCommand.EXZREVRANGE, SafeEncoder.encode(joinKeyAndIndex(i)),
                        toByteArray(startRank), toByteArray(endRank), SafeEncoder.encode(WITHSCORES));
                } else {
                    p.sendCommand(ModuleCommand.EXZRANGE, SafeEncoder.encode(joinKeyAndIndex(i)),
                        toByteArray(startRank), toByteArray(endRank), SafeEncoder.encode(WITHSCORES));
                }
            }

            List<Object> objs = p.syncAndReturnAll();
            for (int i = 0; i < objs.size(); i++) {
                if (objs.get(i) == null) {
                    LOGGER.error("Could not find key: {}", SafeEncoder.encode(joinKeyAndIndex(i)));
                    continue;
                }

                List<String> rangeRets = BuilderFactory.STRING_LIST.build(objs.get(i));
                if (rangeRets != null) {
                    for (int j = 0; j < rangeRets.size(); j += 2) {
                        String member = rangeRets.get(j);
                        String score = rangeRets.get(j + 1);
                        Long rank = null;
                        if (queryRankFromRedis) {
                             rank = rankFor(member);
                        }
                        leaderDataList.add(new LeaderData(member, score, rank));
                    }
                }
            }
        }
        // sort and return sublist
        if (reverse) {
            leaderDataList.sort(Collections.reverseOrder());
        } else {
            Collections.sort(leaderDataList);
        }

        if (queryRankFromRedis) {
            return leaderDataList.subList(0, (int)endRank + 1);
        }

        List<LeaderData> leaderDatas = leaderDataList.subList(0, (int)endRank + 1);
        long rank = useZeroIndexForRank ? 0 : 1;
        for (LeaderData data : leaderDatas) {
            data.setRank(rank++);
        }
        return leaderDatas;
    }

    /**
     * Retrieve a page of leaders from the leaderboard.
     *
     * @param page the page
     * @return a page of leaders from the leaderboard.
     */
    public List<LeaderData> leaders(long page) {
        if (page < 1) {
            page = 1;
        }

        long totalMembers = totalMembers();
        long totalPages = (long)Math.ceil((double)totalMembers / (double)pageSize);
        if (page > totalPages) {
            page = totalPages;
        }

        long startOffset = (page - 1) * pageSize;
        long endOffset = startOffset + pageSize;
        if (endOffset > totalMembers) {
            endOffset = totalMembers;
        }

        List<LeaderData> leaderDataList = top(endOffset);
        return leaderDataList.subList((int)startOffset, (int)endOffset);
    }

    /**
     * Expire the current leaderboard in a set number of seconds.
     *
     * @param seconds the seconds
     * @return Number of seconds after which the leaderboard will be expired.
     */
    public Long expireLeaderBoard(final long seconds) {
        long expires = 0;
        try (Jedis jedis = jedisPool.getResource()) {
            Pipeline p = jedis.pipelined();
            for (int i = 0; i < shardKeySize; i++) {
                p.sendCommand(Command.EXPIRE, SafeEncoder.encode(joinKeyAndIndex(i)), toByteArray(seconds));
            }

            List<Object> objs = p.syncAndReturnAll();
            for (int i = 0; i < objs.size(); i++) {
                expires += BuilderFactory.LONG.build(objs.get(i));
            }
        }
        return expires;
    }

    /**
     * Delete the current leaderboard.
     * @return success: 1, not found: 0
     */
    public Long delLeaderBoard() {
        long dels = 0;
        try (Jedis jedis = jedisPool.getResource()) {
            Pipeline p = jedis.pipelined();
            for (int i = 0; i < shardKeySize; i++) {
                p.sendCommand(Command.DEL, SafeEncoder.encode(joinKeyAndIndex(i)));
            }

            List<Object> objs = p.syncAndReturnAll();
            for (int i = 0; i < objs.size(); i++) {
                dels += BuilderFactory.LONG.build(objs.get(i));
            }
        }
        return dels;
    }
}