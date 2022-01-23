package com.cloud.MapReduce.Data;

import java.util.ArrayList;
import java.util.HashMap;

public class CategoriesTable {
	
	public static String categoriesTable = "categories";

	private static final HashMap<String, Integer> categoriesMap;
	private static final ArrayList<String> categoriesCol;

	static {
		categoriesMap = new HashMap<>();
		categoriesMap.put("id", 0);
		categoriesMap.put("asin", 1);
		categoriesMap.put("category", 2);

		categoriesCol = new ArrayList<>();
		categoriesCol.add("id");
		categoriesCol.add("asin");
		categoriesCol.add("category");

	}

	public static int getColumnIndex(String column) throws IllegalArgumentException {
		if (categoriesMap.containsKey(column))
			return categoriesMap.get(column);
		else
			throw new IllegalArgumentException("Given column does not exist in Similar table");
	}

	public static String getColumnFromIndex(int index) throws IllegalArgumentException {
		if (index < categoriesCol.size())
			return categoriesCol.get(index);
		else
			throw new IllegalArgumentException("Given column does not exist in Similar table");
	}

	public static int getNumOfColumns() {
		return categoriesCol.size();
	}

	public static HashMap<String, Integer> getCategoriesmap() {
		return categoriesMap;
	}

	public static ArrayList<String> getCategoriescol() {
		return categoriesCol;
	}
    
}
