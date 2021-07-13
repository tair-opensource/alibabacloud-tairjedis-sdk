package com.aliyun.tair.tests.leaderboard;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import com.aliyun.tair.leaderboard.LeaderBoard;
import com.aliyun.tair.leaderboard.LeaderData;
import com.aliyun.tair.tests.TestBase;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class LeaderBoardMultiScoreTest extends TestBase {
    private static LeaderBoard lb;
    private static List<LeaderData> leaderDataList;

    @BeforeClass
    public static void beforeClass() {
        lb = new LeaderBoard("leaderboard", jedisPool);
        leaderDataList = new ArrayList<>();
    }

    @Before
    public void before() {
        // clean first
        lb.delLeaderBoard();
        leaderDataList.clear();

        Random r = new Random();
        for (int i = 0; i < 55; i++) {
            double[] scores = new double[3];
            scores[0] = r.nextDouble();
            scores[1] = r.nextDouble();
            scores[2] = r.nextDouble();

            // add lb
            lb.addMember("member_" + i, scores);

            // add DataList
            String scoreStr = LeaderBoard.joinScoresToString(scores);
            LeaderData leaderData = new LeaderData("member_" + i,  scoreStr, 1L);
            leaderDataList.add(leaderData);
        }
    }

    @Test
    public void multi_score_add_test() {
        // get all leaders
        List<LeaderData> top = lb.allLeaders();

        // sort DataList
        Collections.sort(leaderDataList);

        for (int i = 0; i < top.size(); i++) {
            assertEquals(top.get(i).getMember(), leaderDataList.get(i).getMember());
        }
    }

    @Test
    public void change_score_for_test() {
        // diff dimension score will get error
        try {
            lb.incrScoreFor("member_0", "1#1#1#1");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("score is not a valid format"));
        }

        // change score
        String newScore = "1#1#1";
        assertTrue(lb.addMember("member_0", newScore));
        assertEquals(54, (long)lb.rankFor("member_0"));
    }

    @Test
    public void remove_member_test() {
        lb.removeMember("member_0");
        assertNull(lb.rankFor("member_0"));
    }

    @Test
    public void total_members_in_score_range_test() {
        // sort first
        Collections.sort(leaderDataList);

        String minScore = leaderDataList.get(10).getScore();
        String maxScore = leaderDataList.get(20).getScore();
        assertEquals(11, (long)lb.totalMembersInScoreRange(minScore, maxScore));
        assertEquals(55, (long)lb.totalMembers());
    }

    @Test
    public void remove_members_in_score_range_test() {
        // sort first
        Collections.sort(leaderDataList);

        String minScore = leaderDataList.get(10).getScore();
        String maxScore = leaderDataList.get(20).getScore();
        assertEquals(11, (long)lb.removeMembersInScoreRange(minScore, maxScore));
        assertEquals(44, (long)lb.totalMembers());
    }

    @Test
    public void score_for_test() {
        lb.scoreFor("member_0");
        lb.removeMember("member_0");
        assertNull(lb.scoreFor("member_0"));
    }

    @Test
    public void rank_for_test() {
        // sort first
        Collections.sort(leaderDataList);

        String firstMember = leaderDataList.get(0).getMember();

        assertEquals(0, (long)lb.rankFor(firstMember));
        lb.removeMember(firstMember);
        assertNull(lb.rankFor(firstMember));
    }
}
