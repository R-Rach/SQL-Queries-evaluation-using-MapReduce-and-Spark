package com.cloud.MapReduce.Data;

import java.util.ArrayList;
import java.util.HashMap;

public class ReviewsTable {

	public static String reviewsTable = "reviews";

	private static final HashMap<String, Integer> reviewsMap;
	private static final ArrayList<String> reviewsCol;

	static {
		reviewsMap = new HashMap<>();
		reviewsMap.put("id", 0);
		reviewsMap.put("asin", 1);
		reviewsMap.put("reviewdate", 2);
		reviewsMap.put("customerid", 3);
		reviewsMap.put("rating", 4);
		reviewsMap.put("votes", 5);
		reviewsMap.put("helpful", 6);

		reviewsCol = new ArrayList<>();
		reviewsCol.add("id");
		reviewsCol.add("asin");
		reviewsCol.add("reviewdate");
		reviewsCol.add("customerid");
		reviewsCol.add("rating");
		reviewsCol.add("votes");
		reviewsCol.add("helpful");
	}

	public static int getColumnIndex(String column) throws IllegalArgumentException {
		if (reviewsMap.containsKey(column))
			return reviewsMap.get(column);
		else
			throw new IllegalArgumentException("Given column does not exist in Reviews table");
	}

	public static String getColumnFromIndex(int index) throws IllegalArgumentException {
		if (index < reviewsCol.size())
			return reviewsCol.get(index);
		else
			throw new IllegalArgumentException("Given column does not exist in Reviews table");
	}

	public static int getNumOfColumns() {
		return reviewsCol.size();
	}

	public static HashMap<String, Integer> getReviewsmap() {
		return reviewsMap;
	}

	public static ArrayList<String> getReviewscol() {
		return reviewsCol;
	}

}
