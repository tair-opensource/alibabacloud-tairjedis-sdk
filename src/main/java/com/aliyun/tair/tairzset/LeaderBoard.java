package com.aliyun.tair.tairzset;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.tairzset.params.RankParams;
import com.aliyun.tair.util.JoinParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Protocol.Command;
import redis.clients.jedis.util.SafeEncoder;

import static redis.clients.jedis.Protocol.toByteArray;

public class LeaderBoard {
    private static final Logger LOGGER = LoggerFactory.getLogger(LeaderBoard.class);

    private static final String CH = "CH";
    private static final String WITHSCORES = "WITHSCORES";
    private static final int DEFAULT_PAGE_SIZE = 10;
    private static final boolean DEFAULT_REVERSE = false;
    private static final boolean DEFAULT_USE_ZERO_INDEX = true;

    private final String name;
    private final byte[] nameBinary;
    private final JedisPool jedisPool;
    private final int pageSize;
    private final boolean reverse;
    private final boolean useZeroIndexForRank;

    public LeaderBoard(String name, JedisPool jedisPool) {
        this(name, jedisPool, DEFAULT_PAGE_SIZE);
    }

    public LeaderBoard(String name, JedisPool jedisPool, int pageSize) {
        this(name, jedisPool, pageSize, DEFAULT_REVERSE);
    }

    public LeaderBoard(String name, JedisPool jedisPool, int pageSize, boolean reverse) {
        this(name, jedisPool, pageSize, reverse, DEFAULT_USE_ZERO_INDEX);
    }

    public LeaderBoard(String name, JedisPool jedisPool, int pageSize, boolean reverse, boolean useZeroIndexForRank) {
        this.name = name;
        this.nameBinary = SafeEncoder.encode(name);
        this.jedisPool = jedisPool;
        this.pageSize = pageSize;
        this.reverse = reverse;
        this.useZeroIndexForRank = useZeroIndexForRank;
    }

    public static String joinScoresToString(final double... scores) {
        StringBuilder mscore = new StringBuilder();
        for (double score : scores) {
            mscore.append(score);
            mscore.append("#");
        }
        return mscore.substring(0, mscore.length() - 1);
    }

    /**
     * Add a member to leaderboard.
     *
     * @param member the member
     * @param scores the scores
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
            Object obj = jedis.sendCommand(ModuleCommand.EXZADD, nameBinary, SafeEncoder.encode(CH), score, member);
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
                p.sendCommand(ModuleCommand.EXZADD, nameBinary, SafeEncoder.encode(CH),
                    SafeEncoder.encode(entry.getValue()), SafeEncoder.encode(entry.getKey()));
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
        return incrScoreFor(SafeEncoder.encode(member), increment);
    }

    public String incrScoreFor(final byte[] member, final String increment) {
        try (Jedis jedis = jedisPool.getResource()) {
            Object obj = jedis.sendCommand(ModuleCommand.EXZINCRBY, nameBinary, SafeEncoder.encode(increment),
                member);
            return BuilderFactory.STRING.build(obj);
        }
    }

    /**
     * Remove member from leaderboard.
     *
     * @param members the members
     * @return The number of members removed from the leaderboard.
     */
    public Long removeMember(final String... members) {
        return removeMember(SafeEncoder.encodeMany(members));
    }

    public Long removeMember(final byte[]... members) {
        try (Jedis jedis = jedisPool.getResource()) {
            Object obj = jedis.sendCommand(ModuleCommand.EXZREM, JoinParameters.joinParameters(nameBinary, members));
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
        List<LeaderData> leaderDataList = new ArrayList<>();
        Object obj;
        try (Jedis jedis = jedisPool.getResource()) {
            if (reverse) {
                obj = jedis.sendCommand(ModuleCommand.EXZREVRANGE, nameBinary, toByteArray(startOffset),
                    toByteArray(endOffset), SafeEncoder.encode(WITHSCORES));
            } else {
                obj = jedis.sendCommand(ModuleCommand.EXZRANGE, nameBinary, toByteArray(startOffset),
                    toByteArray(endOffset), SafeEncoder.encode(WITHSCORES));
            }
            List<String> rangeRets = BuilderFactory.STRING_LIST.build(obj);
            if (rangeRets != null) {
                for (int i = 0; i < rangeRets.size(); i += 2) {
                    String member = rangeRets.get(i);
                    String score = rangeRets.get(i + 1);
                    Long rank = rankFor(member);
                    leaderDataList.add(new LeaderData(member, score, rank));
                }
            }
        }
        return leaderDataList;
    }

    /**
     * Total members of the leaderboard.
     *
     * @return total members of the leaderboard, 0 if key does not exist.
     */
    public Long totalMembers() {
        try (Jedis jedis = jedisPool.getResource()) {
            Object obj = jedis.sendCommand(ModuleCommand.EXZCARD, nameBinary);
            return BuilderFactory.LONG.build(obj);
        }
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
        try (Jedis jedis = jedisPool.getResource()) {
            Object obj = jedis.sendCommand(ModuleCommand.EXZCOUNT, nameBinary,
                toByteArray(minScore), toByteArray(maxScore));
            return BuilderFactory.LONG.build(obj);
        }
    }

    public Long totalMembersInScoreRange(final String minScore, final String maxScore) {
        try (Jedis jedis = jedisPool.getResource()) {
            Object obj = jedis.sendCommand(ModuleCommand.EXZCOUNT, nameBinary,
                SafeEncoder.encode(minScore), SafeEncoder.encode(maxScore));
            return BuilderFactory.LONG.build(obj);
        }
    }

    /**
     * Remove members from the current leaderboard in a given score range.
     *
     * @param minScore Minimum score
     * @param maxScore Maximum score
     * @return the number of members removed.
     */
    public Long removeMembersInScoreRange(final double minScore, final double maxScore) {
        try (Jedis jedis = jedisPool.getResource()) {
            Object obj = jedis.sendCommand(ModuleCommand.EXZREMRANGEBYSCORE, nameBinary,
                toByteArray(minScore), toByteArray(maxScore));
            return BuilderFactory.LONG.build(obj);
        }
    }

    public Long removeMembersInScoreRange(final String minScore, final String maxScore) {
        try (Jedis jedis = jedisPool.getResource()) {
            Object obj = jedis.sendCommand(ModuleCommand.EXZREMRANGEBYSCORE, nameBinary,
                SafeEncoder.encode(minScore), SafeEncoder.encode(maxScore));
            return BuilderFactory.LONG.build(obj);
        }
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
            Object obj = jedis.sendCommand(ModuleCommand.EXZSCORE, nameBinary, member);
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
        Object obj;
        Long rank;
        try (Jedis jedis = jedisPool.getResource()) {
            if (reverse) {
                obj = jedis.sendCommand(ModuleCommand.EXZREVRANK, rankParams.getByteParams(nameBinary, member));
            } else {
                obj = jedis.sendCommand(ModuleCommand.EXZRANK, rankParams.getByteParams(nameBinary, member));
            }
        }
        rank = BuilderFactory.LONG.build(obj);
        if (rank != null && !useZeroIndexForRank) {
            rank += 1;
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

        if (number < 1) {
            number = 1;
        }
        long totalMembers = totalMembers();
        long startRank = 0;
        long endRank = number > totalMembers ? totalMembers-1 : number-1;

        return retrieveMember(startRank, endRank);
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

        long totalPage = totalPages();
        if (page > totalPage) {
            page = totalPage;
        }

        long startOffset = (page - 1) * pageSize;
        long endOffset = startOffset + pageSize - 1;

        return retrieveMember(startOffset, endOffset);
    }

    /**
     * Retrieves all leaders from the named leaderboard.
     *
     * @return the named leaderboard.
     */
    public List<LeaderData> allLeaders() {
        long startOffset = 0;
        long endOffset = -1;
        return retrieveMember(startOffset, endOffset);
    }

    /**
     * Expire the current leaderboard in a set number of seconds.
     *
     * @param seconds the seconds
     * @return Number of seconds after which the leaderboard will be expired.
     */
    public Long expireLeaderBoard(final long seconds) {
        try (Jedis jedis = jedisPool.getResource()) {
            Object obj = jedis.sendCommand(Command.EXPIRE, nameBinary, toByteArray(seconds));
            return BuilderFactory.LONG.build(obj);
        }
    }

    /**
     * Delete the current leaderboard.
     * @return success: 1, not found: 0
     */
    public Long delLeaderBoard() {
        try (Jedis jedis = jedisPool.getResource()) {
            Object obj = jedis.sendCommand(Command.DEL, nameBinary);
            return BuilderFactory.LONG.build(obj);
        }
    }

}