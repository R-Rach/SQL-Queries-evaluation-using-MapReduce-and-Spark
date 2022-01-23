package com.cloud.MapReduce.SQL;

import java.util.ArrayList;
import java.util.StringTokenizer;

import com.cloud.MapReduce.Jobs.SparkJob;
import com.cloud.MapReduce.Model.QueryModel;

public class SqlQueryParser {

	/**
	 * Method to parse the Query and generate relevant tokens from it
	 * 
	 * @param parsedQuery
	 */
	public static void parse(QueryModel parsedQuery) {
		if (parsedQuery.getQuery() == null) {
			parsedQuery.setParsed(false);
			return;
		}

		String[] list = parsedQuery.getQuery().split("SELECT|FROM|WHERE|GROUP BY|HAVING");

		// list size = 6
		// list[0] = "" so ignore

		// SELECT list[1] - select columns
		StringTokenizer stSelect = new StringTokenizer(list[1].trim(), ", ", false);
		ArrayList<String> selectColumns = new ArrayList<>();
		while (stSelect.hasMoreTokens()) {
			String token = stSelect.nextToken();
			selectColumns.add(token.trim().toLowerCase());
		}
		parsedQuery.setSelectColumns(selectColumns);

		// FROM list[2] - table name
		parsedQuery.setTable(list[2].trim().toLowerCase());

		// WHERE list[3]
		String[] whereList = list[3].trim().split("=", 2);
		// getting whereComparator column
		parsedQuery.setWhereComparator(whereList[0].trim().toLowerCase());
		// getting whereComparison --- string as it is
		if (isWithoutQuotes(whereList[1].trim())) {
			parsedQuery.setWhereComparision(whereList[1].trim());
		} else {
			parsedQuery.setWhereComparision(whereList[1].trim().substring(1, whereList[1].trim().length() - 1));
		}

		// GROUP BY list[4] - grp by cols
		StringTokenizer stGrpBy = new StringTokenizer(list[4].trim(), ", ", false);
		ArrayList<String> groupbyColumns = new ArrayList<>();
		while (stGrpBy.hasMoreTokens()) {
			String token = stGrpBy.nextToken();
			groupbyColumns.add(token.trim().toLowerCase());
		}
		parsedQuery.setGroupbyColumns(groupbyColumns);

		// HAVING list[5] -
		// getting aggregate func
		StringTokenizer sthaving = new StringTokenizer(list[5].trim(), "() ", false);
		parsedQuery.setAggregateFunction(sthaving.nextToken().trim().toUpperCase());
		// getting comparision col
		parsedQuery.setComparisonColumn(sthaving.nextToken().trim().toLowerCase());
		// getting comparison type -- as it is > < >= <=
		parsedQuery.setComparisonType(sthaving.nextToken().trim());
		// getting comparison number -- in integer
		parsedQuery.setComparisonNumber(Double.parseDouble(sthaving.nextToken().trim()));

		// is parsed
		parsedQuery.setParsed(true);
	}

	public static boolean isWithoutQuotes(String strNum) {
		if (strNum.charAt(0) == '"' && strNum.charAt(strNum.length() - 1) == '"')
			return false;
		else
			return true;
	}
}
