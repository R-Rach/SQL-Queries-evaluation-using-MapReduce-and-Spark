package com.cloud.MapReduce.Model;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class OutputModel {

	private String mapreduceExecutionTime;
	private String mapperInput;
	private String mapperOutput;
	private String reducerInput;
	private String reducerOutput;

	private String sparkExecutionTime;
	private String sparkTransformations;
	
	private String _QueryResult_;

	public OutputModel() {

	}

	public String getMapReduceExecutionTime() {
		return mapreduceExecutionTime;
	}

	public void setMapReduceExecutionTime(String mapreduceExecutionTime) {
		this.mapreduceExecutionTime = mapreduceExecutionTime;
	}

	public String getSparkExecutionTime() {
		return sparkExecutionTime;
	}

	public void setSparkExecutionTime(String sparkExecutionTime) {
		this.sparkExecutionTime = sparkExecutionTime;
	}

	public String getMapperInput() {
		return mapperInput;
	}

	public void setMapperInput(String mapperInput) {
		this.mapperInput = mapperInput;
	}

	public String getReducerInput() {
		return reducerInput;
	}

	public void setReducerInput(String reducerInput) {
		this.reducerInput = reducerInput;
	}

	public String getMapperOutput() {
		return mapperOutput;
	}

	public void setMapperOutput(String mapperOutput) {
		this.mapperOutput = mapperOutput;
	}

	public String getReducerOutput() {
		return reducerOutput;
	}

	public void setReducerOutput(String reducerOutput) {
		this.reducerOutput = reducerOutput;
	}

	public String getSparkTransformations() {
		return sparkTransformations;
	}

	public void setSparkTransformations(String sparkTransformations) {
		this.sparkTransformations = sparkTransformations;
	}
	
	public String getQueryResult() {
		return _QueryResult_;
	}

	public void setQueryResult(String queryResult) {
		this._QueryResult_ = queryResult;
	}

	@Override
	public String toString() {
		return "OutputModel \n[\n   hadoopExecutionTime= " + mapreduceExecutionTime + ", \n   mapperInput= " + mapperInput
				+ ", \n   mapperOutput= " + mapperOutput + ", \n   reducerInput= " + reducerInput
				+ ", \n   reducerOutput= " + reducerOutput + ", \n   sparkExecutionTime= " + sparkExecutionTime
				+ ", \n   sparkTransformations= " + sparkTransformations 
				+ ", \n   queryResult= " + _QueryResult_ + "\n]";
	}

}
