package com.cloud.MapReduce.Model;

public class InputModel {

	private String query;

	public InputModel() {
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	@Override
	public String toString() {
		return "InputModel [query=" + query + "]";
	}

}
