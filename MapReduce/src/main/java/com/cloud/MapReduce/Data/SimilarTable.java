package com.cloud.MapReduce.Data;

import java.util.ArrayList;
import java.util.HashMap;

public class SimilarTable {

	public static String similarTable = "products";

	private static final HashMap<String, Integer> similarMap;
	private static final ArrayList<String> similarCol;

	static {
		similarMap = new HashMap<>();
		similarMap.put("id", 0);
		similarMap.put("asin", 1);
		similarMap.put("similarasin", 2);

		similarCol = new ArrayList<>();
		similarCol.add("id");
		similarCol.add("asin");
		similarCol.add("similarasin");

	}

	public static int getColumnIndex(String column) throws IllegalArgumentException {
		if (similarMap.containsKey(column))
			return similarMap.get(column);
		else
			throw new IllegalArgumentException("Given column does not exist in Similar table");
	}

	public static String getColumnFromIndex(int index) throws IllegalArgumentException {
		if (index < similarCol.size())
			return similarCol.get(index);
		else
			throw new IllegalArgumentException("Given column does not exist in Similar table");
	}

	public static int getNumOfColumns() {
		return similarCol.size();
	}

	public static HashMap<String, Integer> getSimilarmap() {
		return similarMap;
	}

	public static ArrayList<String> getSimilarcol() {
		return similarCol;
	}

}
