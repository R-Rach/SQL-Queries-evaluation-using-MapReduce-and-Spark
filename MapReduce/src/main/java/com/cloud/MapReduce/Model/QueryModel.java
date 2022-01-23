package com.cloud.MapReduce.Model;

import java.util.ArrayList;

/**
 * Contains relevant tokens for parsing a query
 * 
 * SELECT <COLUMNS> {selectColumns}, {aggregateFunction} FUNC(COLUMN1 {comparisonColumn})
 * FROM <TABLE> {table} 
 * WHERE <COLUMN> {whereComparator} = Y {whereComparision}
 * GROUP BY <COLUMNS> {groupbyColumns} 
 * HAVING {aggregateFunction} FUNC(COLUMN1{comparisonColumn}) > {comparisonType} X {comparisonNumber}
 *  -- Here FUNC can be COUNT, MAX, MIN, SUM
 */

public class QueryModel {

	private String query;

	private ArrayList<String> selectColumns;

	private String table;

	private String whereComparator;

	private String whereComparision;

	private ArrayList<String> groupbyColumns;

	private String aggregateFunction;

	private String comparisonColumn;
	private String comparisonType;
	private double comparisonNumber;

	private boolean parsed;

	public QueryModel(String query) {
		this.query = query;
		this.selectColumns = new ArrayList<>();
		this.table = null;
		this.whereComparator = null;
		this.whereComparision = null;
		this.groupbyColumns = new ArrayList<>();
		this.aggregateFunction = null;
		this.comparisonColumn = null;
		this.comparisonType = null;
		this.comparisonNumber = 0;
		this.parsed = false;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public ArrayList<String> getSelectColumns() {
		return selectColumns;
	}

	public void setSelectColumns(ArrayList<String> selectColumns) {
		this.selectColumns = selectColumns;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public String getWhereComparator() {
		return whereComparator;
	}

	public void setWhereComparator(String whereComparator) {
		this.whereComparator = whereComparator;
	}

	public String getWhereComparision() {
		return whereComparision;
	}

	public void setWhereComparision(String whereComparision) {
		this.whereComparision = whereComparision;
	}

	public ArrayList<String> getGroupbyColumns() {
		return groupbyColumns;
	}

	public void setGroupbyColumns(ArrayList<String> groupbyColumns) {
		this.groupbyColumns = groupbyColumns;
	}

	public String getAggregateFunction() {
		return aggregateFunction;
	}

	public void setAggregateFunction(String aggregateFunction) {
		this.aggregateFunction = aggregateFunction;
	}

	public String getComparisonColumn() {
		return comparisonColumn;
	}

	public void setComparisonColumn(String comparisonColumn) {
		this.comparisonColumn = comparisonColumn;
	}

	public String getComparisonType() {
		return comparisonType;
	}

	public void setComparisonType(String comparisonType) {
		this.comparisonType = comparisonType;
	}

	public double getComparisonNumber() {
		return comparisonNumber;
	}

	public void setComparisonNumber(double comparisonNumber) {
		this.comparisonNumber = comparisonNumber;
	}

	public boolean isParsed() {
		return parsed;
	}

	public void setParsed(boolean parsed) {
		this.parsed = parsed;
	}

	@Override
	public String toString() {
		return "QueryModel \n[\n   query= " + query + ", \n   selectColumns= " + selectColumns + ", \n   table= "
				+ table + ", \n   whereComparator= " + whereComparator + ", \n   whereComparision= -->" + whereComparision +"<--"
				+ ", \n   groupbyColumns= " + groupbyColumns + ", \n   aggregateFunction= " + aggregateFunction
				+ ", \n   comparisonColumn= " + comparisonColumn + ", \n   comparisonType= " + comparisonType
				+ ", \n   comparisonNumber= " + comparisonNumber + ", \n   parsed= " + parsed + "\n]";
	}
}
