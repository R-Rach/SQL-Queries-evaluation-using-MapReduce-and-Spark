package com.cloud.MapReduce.Data;

import java.util.ArrayList;

public class DataExtractor {

	/**
	 * @param table
	 * @param column
	 * @return index of given column name in @param table
	 * @throws IllegalArgumentException
	 */
	public static int getColumnIndex(String table, String column) throws IllegalArgumentException {
		if (table.equalsIgnoreCase("reviews")) {
			return ReviewsTable.getColumnIndex(column);
		}
		if (table.equalsIgnoreCase("products")) {
			return ProductsTable.getColumnIndex(column);
		}
		if (table.equalsIgnoreCase("similar")) {
			return SimilarTable.getColumnIndex(column);
		}
        if(table.equalsIgnoreCase("categories")) {
        	return CategoriesTable.getColumnIndex(column);
        }
		else
			throw new IllegalArgumentException(table + " table does not exist");
	}

	/**
	 * @param table
	 * @param index
	 * @return column name from given integer index in @param table
	 * @throws IllegalArgumentException
	 */
	public static String getColumnFromIndex(String table, int index) throws IllegalArgumentException {
		if (table.equalsIgnoreCase("reviews")) {
			return ReviewsTable.getColumnFromIndex(index);
		}
		if (table.equalsIgnoreCase("products")) {
			return ProductsTable.getColumnFromIndex(index);
		}
		if (table.equalsIgnoreCase("similar")) {
			return SimilarTable.getColumnFromIndex(index);
		}
    	if(table.equalsIgnoreCase("categories")) {
        	return CategoriesTable.getColumnFromIndex(index);
        }
		else
			throw new IllegalArgumentException(table + " table does not exist");
	}

	/**
	 * @param table
	 * @return total number of columns in @param table
	 */
	public static int getNumOfColumns(String table) {
		if (table.equalsIgnoreCase("reviews")) {
			return ReviewsTable.getNumOfColumns();
		}
		if (table.equalsIgnoreCase("products")) {
			return ProductsTable.getNumOfColumns();
		}
		if (table.equalsIgnoreCase("similar")) {
			return SimilarTable.getNumOfColumns();
		}
    	if(table.equalsIgnoreCase("categories")) {
        	return CategoriesTable.getNumOfColumns();
        }
		else
			throw new IllegalArgumentException(table + " table does not exist");
	}

	public static ArrayList<String> getListOfCols(String table) {
		if (table.equalsIgnoreCase("reviews")) {
			return ReviewsTable.getReviewscol();
		}
		if (table.equalsIgnoreCase("products")) {
			return ProductsTable.getProductscol();
		}
		if (table.equalsIgnoreCase("similar")) {
			return SimilarTable.getSimilarcol();
		}
    	if(table.equalsIgnoreCase("categories")) {
        	return CategoriesTable.getCategoriescol();
        }
		else
			throw new IllegalArgumentException(table + " table does not exist");
	}
}
