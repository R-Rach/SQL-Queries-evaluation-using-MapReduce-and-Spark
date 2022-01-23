package com.cloud.MapReduce.Data;

import java.util.ArrayList;
import java.util.HashMap;

public class ProductsTable {

	public static String productsTable = "products";

	private static final HashMap<String, Integer> productsMap;
	private static final ArrayList<String> productsCol;

	static {
		productsMap = new HashMap<>();
		productsMap.put("id", 0);
		productsMap.put("asin", 1);
		productsMap.put("title", 2);
		productsMap.put("group", 3);
		productsMap.put("salesrank", 4);
		productsMap.put("nosimilar", 5);
		productsMap.put("nocategories", 6);
		productsMap.put("totalreviews", 7);
		productsMap.put("downloaded", 8);
		productsMap.put("avgrating", 9);

		productsCol = new ArrayList<>();
		productsCol.add("id");
		productsCol.add("asin");
		productsCol.add("title");
		productsCol.add("group");
		productsCol.add("salesrank");
		productsCol.add("similar");
		productsCol.add("categories");
		productsCol.add("totalreviews");
		productsCol.add("downloaded");
		productsCol.add("avgrating");
	}

	public static int getColumnIndex(String column) throws IllegalArgumentException {
		if (productsMap.containsKey(column))
			return productsMap.get(column);
		else
			throw new IllegalArgumentException("Given column does not exist in Products table");
	}

	public static String getColumnFromIndex(int index) throws IllegalArgumentException {
		if (index < productsCol.size())
			return productsCol.get(index);
		else
			throw new IllegalArgumentException("Given column does not exist in Products table");
	}

	public static int getNumOfColumns() {
		return productsCol.size();
	}

	public static HashMap<String, Integer> getProductsmap() {
		return productsMap;
	}

	public static ArrayList<String> getProductscol() {
		return productsCol;
	}

}
