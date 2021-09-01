package com.aliyun.tair.tests.leaderboard;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.tair.tairzset.LeaderBoard;
import com.aliyun.tair.tairzset.LeaderData;
import com.aliyun.tair.tests.TestBase;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class LeaderBoardTest extends TestBase {
    private static LeaderBoard lb;

    @BeforeClass
    public static void beforeClass() {
        lb = new LeaderBoard("leaderboard", jedisPool);
    }

    @Before
    public void before() {
        // del first
        lb.delLeaderBoard();

        for (int i = 0; i < 55; i++) {
            lb.addMember("member_" + i , i);
        }
    }

    @Test
    public void add_multi_member_test() {
        Map<String, String> memberScores = new HashMap<String, String>() {{
            put("tom", "10");
            put("jonny", "20");
            put("danny", "30");
        }};
        lb.addMember(memberScores);
        assertEquals("10", lb.scoreFor("tom"));
        assertEquals(58, (long)lb.totalMembers());
    }

    @Test
    public void change_score_for_test() {
        lb.incrScoreFor("member_0", "100");
        assertEquals("100", lb.scoreFor("member_0"));
    }

    @Test
    public void remove_member_test() {
        lb.removeMember("member_0");
        assertEquals(54, (long)lb.totalMembers());
    }

    @Test
    public void retrieve_member_test() {
        List<LeaderData> retrievedMembers = lb.retrieveMember(10, 19);
        assertEquals(10, retrievedMembers.size());
        for (int i = 0; i < retrievedMembers.size(); i++) {
            assertEquals(String.valueOf(i + 10), retrievedMembers.get(i).getScore());
        }
    }

    @Test
    public void total_members() {
        assertEquals(55, (long)lb.totalMembers());
    }

    @Test
    public void total_pages() {
        assertEquals(6, (long)lb.totalPages());
    }

    @Test
    public void total_members_in_score_range_test() {
        assertEquals(11, (long)lb.totalMembersInScoreRange(10, 20));
        assertEquals(55, (long)lb.totalMembersInScoreRange(0, 100));
    }

    @Test
    public void remove_members_in_score_range_test() {
        assertEquals(11, (long)lb.removeMembersInScoreRange(10, 20));
        assertEquals(44, (long)lb.removeMembersInScoreRange(0, 100));
    }

    @Test
    public void score_for_test() {
        assertEquals("0", lb.scoreFor("member_0"));
        lb.removeMember("member_0");
        assertNull(lb.scoreFor("member_0"));
    }

    @Test
    public void rank_for_test() {
        assertEquals(54, (long)lb.rankFor("member_54"));
        lb.removeMember("member_54");
        assertNull(lb.scoreFor("member_54"));
    }

    @Test
    public void score_and_rank_for_test() {
        LeaderData leaderData = lb.scoreAndRankFor("member_0");
        assertEquals(0, (long)leaderData.getRank());
        assertEquals("0", leaderData.getScore());
        assertEquals("member_0", leaderData.getMember());
    }

    @Test
    public void top_i_test() {
        List<LeaderData> tops = lb.top(22);
        assertEquals(22, tops.size());
        for (int i = 0; i < tops.size(); i++) {
            assertEquals(i + "", tops.get(i).getScore());
        }
    }

    @Test
    public void leaders_i_test() {
        List<LeaderData> leaders = lb.leaders(2);
        assertEquals(10, leaders.size());
        for (int i = 0; i < leaders.size(); i++) {
            assertEquals(String.valueOf(i + 10), leaders.get(i).getScore());
        }
    }

    @Test
    public void expire_leaderboard_test() throws Exception {
        lb.expireLeaderBoard(1);
        Thread.sleep(2000);
        assertEquals(0, (long)lb.delLeaderBoard());
    }

}
