package com.aliyun.tair.tests.leaderboard;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.tair.leaderboard.DistributedLeaderBoard;
import com.aliyun.tair.leaderboard.LeaderData;
import com.aliyun.tair.tests.TestBase;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class DistributedLeaderBoardTest extends TestBase {
    private static DistributedLeaderBoard dlb;

    @BeforeClass
    public static void beforeClass() {
        dlb = new DistributedLeaderBoard("distributed_leaderboard", jedisPool);
    }

    @Before
    public void before() {
        // del first
        dlb.delLeaderBoard();

        for (int i = 0; i < 55; i++) {
            dlb.addMember("member_" + i , i);
        }
    }

    @Test
    public void add_multi_member_test() {
        Map<String, String> memberScores = new HashMap<String, String>() {{
            put("tom", "10");
            put("jonny", "20");
            put("danny", "30");
        }};
        dlb.addMember(memberScores);
        assertEquals("10", dlb.scoreFor("tom"));
        assertEquals(58, (long)dlb.totalMembers());
    }

    @Test
    public void change_score_for_test() {
        dlb.incrScoreFor("member_0", "100");
        assertEquals("100", dlb.scoreFor("member_0"));
    }

    @Test
    public void remove_member_test() {
        dlb.removeMember("member_0");
        assertEquals(54, (long)dlb.totalMembers());
    }

    @Test
    public void total_members() {
        assertEquals(55, (long)dlb.totalMembers());
    }

    @Test
    public void total_pages() {
        assertEquals(6, (long)dlb.totalPages());
    }

    @Test
    public void total_members_in_score_range_test() {
        assertEquals(11, (long)dlb.totalMembersInScoreRange(10, 20));
        assertEquals(55, (long)dlb.totalMembersInScoreRange(0, 100));
    }

    @Test
    public void remove_members_in_score_range_test() {
        assertEquals(11, (long)dlb.removeMembersInScoreRange(10, 20));
        assertEquals(44, (long)dlb.removeMembersInScoreRange(0, 100));
    }

    @Test
    public void score_for_test() {
        assertEquals("0", dlb.scoreFor("member_0"));
        dlb.removeMember("member_0");
        assertNull(dlb.scoreFor("member_0"));
    }

    @Test
    public void rank_for_test() {
        for (int i = 0; i < 55; i++) {
            assertEquals(i, (long)dlb.rankFor("member_" + i));
        }
    }

    @Test
    public void score_and_rank_for_test() {
        LeaderData leaderData = dlb.scoreAndRankFor("member_0");
        assertEquals(0, (long)leaderData.getRank());
        assertEquals("0", leaderData.getScore());
        assertEquals("member_0", leaderData.getMember());
    }

    @Test
    public void top_i_test() {
        List<LeaderData> tops = dlb.top(55);
        assertEquals(55, tops.size());
        for (int i = 0; i < tops.size(); i++) {
            assertEquals(i + "", tops.get(i).getScore());
        }
    }

    @Test
    public void leaders_i_test() {
        List<LeaderData> leaders = dlb.leaders(2);
        assertEquals(10, leaders.size());
        for (int i = 0; i < leaders.size(); i++) {
            assertEquals(String.valueOf(i + 10), leaders.get(i).getScore());
        }
    }

    @Test
    public void expire_leaderboard_test() throws Exception {
        dlb.expireLeaderBoard(1);
        Thread.sleep(2000);
        assertEquals(0, (long)dlb.delLeaderBoard());
    }

    @Test
    public void rank_reverse_test() {
        DistributedLeaderBoard idlb =
            new DistributedLeaderBoard("rank_reverse_test", jedisPool, 10, 10, true);
        for (int i = 0; i < 55; i++) {
            idlb.addMember("member_" + i, i);
        }

        for (int i = 0; i < 55; i++) {
            assertEquals(55 - i - 1, (long)idlb.rankFor("member_" + i));
        }
    }
}
