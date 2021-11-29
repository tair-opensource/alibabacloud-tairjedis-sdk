package com.aliyun.tair.tairzset;

import java.util.ArrayList;
import java.util.List;

public class LeaderData implements Comparable<LeaderData> {
	private String member;
	private String score;
	private Long rank;

	/**
	 * Store leader data
	 *
	 * @param member Name
	 * @param score Score
	 * @param rank Rank
	 */
	public LeaderData(String member, String score, Long rank) {
		this.member = member;
		this.score = score;
		this.rank = rank;
	}

	public LeaderData(String member, String score) {
		this.member = member;
		this.score = score;
	}

	public String getMember() {
		return member;
	}

	public void setMember(String member) {
		this.member = member;
	}

	public String getScore() {
		return score;
	}

	public void setScore(String score) {
		this.score = score;
	}

	public Long getRank() {
		return rank;
	}

	public void setRank(Long rank) {
		this.rank = rank;
	}

	List<Double> parseScoreFromStr(final String scores) {
		String[] splits = scores.split("#");
		List<Double> doubles = new ArrayList<>();
		for (String s : splits) {
			doubles.add(Double.parseDouble(s));
		}
		return doubles;
	}

	@Override
	public int compareTo(LeaderData o) {
		List<Double> local = parseScoreFromStr(getScore());
		List<Double> compare = parseScoreFromStr(o.getScore());
		for (int i = 0; i < local.size(); i++) {
			Double a = local.get(i), b = compare.get(i);
			if (a < b) {
				return -1;
			} else if (a > b) {
				return 1;
			}
		}
		return 0;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder("{");
		sb.append("\"member\":\"")
			.append(member).append('\"');
		sb.append(",\"score\":\"")
			.append(score).append('\"');
		sb.append(",\"rank\":")
			.append(rank);
		sb.append('}');
		return sb.toString();
	}
}
