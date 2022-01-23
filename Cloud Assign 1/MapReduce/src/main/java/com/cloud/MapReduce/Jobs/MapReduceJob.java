package com.cloud.MapReduce.Jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Time;

import com.cloud.MapReduce.GlobalVariables;
import com.cloud.MapReduce.Data.DataExtractor;
import com.cloud.MapReduce.Model.OutputModel;
import com.cloud.MapReduce.Model.QueryModel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.StringTokenizer;

public class MapReduceJob {

	/**
	 * Execute Map-Reduce job for the parsed query
	 * 
	 * @param parsedQuery instance of {@link QueryModel.java} which has relevant
	 *                    tokens to be parsed from SQL query
	 * @return an instance of {@link OutputModel.java} having relevant fields for
	 *         output
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 * @throws IllegalArgumentException
	 */
	public static OutputModel executeMapReduce(QueryModel parsedQuery)
			throws IOException, InterruptedException, ClassNotFoundException, IllegalArgumentException {

		OutputModel outputModel = new OutputModel();

		Configuration conf = new Configuration();

		// configuration properties
		String[] selCol = new String[0];
		selCol = parsedQuery.getSelectColumns().toArray(selCol);
		conf.setStrings("selectColumns", selCol);
		conf.set("table", parsedQuery.getTable());
		conf.set("whereComparator", parsedQuery.getWhereComparator());
		conf.set("whereComparision", parsedQuery.getWhereComparision());
		String[] grpbyCols = new String[0];
		grpbyCols = parsedQuery.getGroupbyColumns().toArray(selCol);
		conf.setStrings("groupbyColumns", grpbyCols);
		conf.set("aggregateFunction", parsedQuery.getAggregateFunction());
		conf.set("comparisonColumn", parsedQuery.getComparisonColumn());
		conf.set("comparisonType", parsedQuery.getComparisonType());
		conf.setDouble("comparisonNumber", parsedQuery.getComparisonNumber());

		// creating job
		Job job = Job.getInstance(conf, "GroupBy");
		job.setJarByClass(MapReduceJob.class);

		// defining Reducer class
		job.setReducerClass(MapReduceReducer.class);

		// defining output key/value type
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// defining input file path,
		// specifying how a Mapper will read the input data-> TextInputFormat.class so
		// that a single line is read by the mapper at a time from the input text file
		// defining Mapper class
		MultipleInputs.addInputPath(job,
				new Path(GlobalVariables.dataPath + parsedQuery.getTable() + ".csv"), TextInputFormat.class, MapReduceMapper.class);

		// defining Output path for saving results
		Path outputPath = new Path(GlobalVariables.mapReducceOutputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		// deleting existing outputPath file if exists already
		outputPath.getFileSystem(conf).delete(outputPath, true);

		long startTime = Time.now();
		long endTime = (job.waitForCompletion(true) ? Time.now() : startTime);
		long execTime = endTime - startTime;

		// writing time of execution in output model
		outputModel.setMapReduceExecutionTime(execTime + " milliseconds");

		// input format of key-value pair to mapper
		StringBuilder mapInpStr = new StringBuilder("<line_no, (");

		// append all columns of parsedQuery.getTable()
		ArrayList<String> listOfCols = DataExtractor.getListOfCols(parsedQuery.getTable());
		for (int i = 0; i < listOfCols.size() - 1; i++) {
			mapInpStr.append(listOfCols.get(i)).append(",");
		}
		mapInpStr.append(listOfCols.get(listOfCols.size() - 1)).append(")>");

		// set the value in output model
		outputModel.setMapperInput(mapInpStr.toString());

		// mapper key-value output format
		StringBuilder mapOutStr = new StringBuilder("<(");

		// append all parseQuery.getSelectCloumns()
		for (int i = 0; i < parsedQuery.getSelectColumns().size() - 2; i++) {
			mapOutStr.append(parsedQuery.getSelectColumns().get(i)).append(", ");
		}
		mapOutStr.append(parsedQuery.getSelectColumns().get(parsedQuery.getSelectColumns().size() - 2));

		mapOutStr.append("), ");

		// append according to agg function
		if (parsedQuery.getAggregateFunction().equalsIgnoreCase("COUNT")) {
			mapOutStr.append("1>");
		} else {
			mapOutStr.append(parsedQuery.getComparisonColumn()).append(">");
		}

		// set the value in output model
		outputModel.setMapperOutput(mapOutStr.toString());

		// reducer key-value input format
		StringBuilder redInpStr = new StringBuilder("<(");

		// append all parseQuery.getSelectCloumns()
		for (int i = 0; i < parsedQuery.getSelectColumns().size() - 2; i++) {
			redInpStr.append(parsedQuery.getSelectColumns().get(i)).append(", ");
		}
		redInpStr.append(parsedQuery.getSelectColumns().get(parsedQuery.getSelectColumns().size() - 2));

		redInpStr.append("), {");

		// append according to agg function
		if (parsedQuery.getAggregateFunction().equalsIgnoreCase("COUNT")) {
			redInpStr.append("1, 1, 1, ... 1}>");
		} else {
			redInpStr.append(parsedQuery.getComparisonColumn()).append("(1), ")
					.append(parsedQuery.getComparisonColumn()).append("(2), ... ")
					.append(parsedQuery.getComparisonColumn()).append("(n)}>");
		}

		// set the value in output model
		outputModel.setReducerInput(redInpStr.toString());

		// reducer key-value pait output format
		StringBuilder redOutStr = new StringBuilder("<(");

		// append all parseQuery.getSelectCloumns()
		for (int i = 0; i < parsedQuery.getSelectColumns().size() - 2; i++) {
			redOutStr.append(parsedQuery.getSelectColumns().get(i)).append(", ");
		}
		redOutStr.append(parsedQuery.getSelectColumns().get(parsedQuery.getSelectColumns().size() - 2));

		redOutStr.append("), ").append(parsedQuery.getSelectColumns().get(parsedQuery.getSelectColumns().size() - 1))
				.append(">");

		// set the value in output model
		outputModel.setReducerOutput(redOutStr.toString());

		return outputModel;
	}

	/**
	 * Mapper class
	 * 
	 */
	private static class MapReduceMapper extends Mapper<Object, Text, Text, Text> {

		private static String[] selectColumns;
		private static String aggFunc;
		private static String table;
		private static String whereComparator;
		private static String whereComparision;
		private static String comparisonColumn;
		private static String comparisonType;
		private static double comparisonNumber;

		/**
		 * Initial setup of the Mapper Job.
		 *
		 * @param context instance of {@link org.apache.hadoop.mapreduce.Mapper.Context}
		 * @throws IOException
		 * @throws InterruptedException
		 */
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			selectColumns = conf.getStrings("selectColumns");
			aggFunc = conf.get("aggregateFunction", "none");
			table = conf.get("table", "none");
			whereComparator = conf.get("whereComparator", "none");
			whereComparision = conf.get("whereComparision", "none");
			comparisonColumn = conf.get("comparisonColumn", "none");
			comparisonType = conf.get("comparisonType", "none");
			comparisonNumber = conf.getDouble("comparisonNumber", Double.MIN_VALUE);
			super.setup(context);
		}

		/**
		 * Map Method
		 * 
		 * Input tuple is of the form (lineNumber, listOfColumns) output tuple is of the
		 * form (listofGroupByColumns, value). Here, the listOfGroupByColumns is a
		 * concatenation of the values of all columns in the group by query, value is
		 * the actual value from the input for aggregate functions of the type Max(),
		 * Min() or Sum(), while it is 1 for the aggregate function count().
		 *
		 * @param key     Input key of the map job.
		 * @param value   Input value for the map job. Here it will be a concatenation
		 *                of the values of all columns, separated by a comma
		 * @param context instance of {@link org.apache.hadoop.mapreduce.Mapper.Context}
		 * @throws IOException
		 * @throws InterruptedException
		 */
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			// take a line and split by ","
//			String[] line = value.toString().split("^");
			StringTokenizer st = new StringTokenizer(value.toString(), "^", false);
			
			ArrayList<String> arr = new ArrayList<String>();
			while(st.hasMoreTokens()) {
				arr.add(st.nextToken().toString());
			}
			String[] line = new String[0];
			line = arr.toArray(line);
			
			// where comparision
			String data = line[DataExtractor.getColumnIndex(table, whereComparator)];
			if (!data.equals(whereComparision)) {
				return;
			}

			// output key contaning all grp by cols
			StringBuilder builder = new StringBuilder(line[DataExtractor.getColumnIndex(table, selectColumns[0])]);
			for (int i = 1; i < selectColumns.length - 1; i++) {
				builder.append(",").append(line[DataExtractor.getColumnIndex(table, selectColumns[i])]);
			}
			
			Text outKey = new Text(builder.toString());

			if (aggFunc.equalsIgnoreCase("COUNT")) {
				context.write(outKey, new Text("1"));
				return;
			} else {
				String aggColData = line[DataExtractor.getColumnIndex(table, comparisonColumn)];
				if (aggFunc.equalsIgnoreCase("SUM") || aggFunc.equalsIgnoreCase("MAX") || aggFunc.equalsIgnoreCase("MIN")) {
					context.write(outKey, new Text(aggColData));
				}
			}
		}
	}

	/**
	 * Reducer class
	 * 
	 */
	private static class MapReduceReducer extends Reducer<Text, Text, Text, Text> {

		private static String[] selectColumns;
		private static String aggFunc;
		private static String table;
		private static String comparisonColumn;
		private static String comparisonType;
		private static double comparisonNumber;

		/**
		 * Initial setup of the Mapper Job.
		 *
		 * @param context instance of {@link org.apache.hadoop.mapreduce.Mapper.Context}
		 * @throws IOException
		 * @throws InterruptedException
		 */
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			selectColumns = conf.getStrings("selectColumns");
			aggFunc = conf.get("aggregateFunction", "none");
			table = conf.get("table", "none");
			comparisonColumn = conf.get("comparisonColumn", "none");
			comparisonType = conf.get("comparisonType", "none");
			comparisonNumber = conf.getDouble("comparisonNumber", Double.MIN_VALUE);
			super.setup(context);
		}

		/**
		 * Reduce method
		 * 
		 * Output key is same as the input key from map. Output value is generated by
		 * applying the required aggregate function on the list input values.
		 * 
		 * @param key     Input key which is same as the output key of the map job
		 * @param values  list of input values after combining for a particular key
		 * @param context instance of
		 *                {@link org.apache.hadoop.mapreduce.Reducer.Context}
		 * @throws IOException
		 * @throws InterruptedExceptions
		 */
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			Iterator<Text> it = values.iterator();

			if (aggFunc.equalsIgnoreCase("MIN")) {
				double min = Double.MAX_VALUE;
				while (it.hasNext()) {
					min = Math.min(min, Double.parseDouble(it.next().toString()));
				}
				if (comparisonType.equals(">")) {
					if (min > comparisonNumber)
						context.write(key, new Text(Double.toString(min)));
				}
				else if (comparisonType.equals(">=")) {
					if (min >= comparisonNumber)
						context.write(key, new Text(Double.toString(min)));
				}
				else if (comparisonType.equals("<")) {
					if (min < comparisonNumber)
						context.write(key, new Text(Double.toString(min)));
				}
				else if (comparisonType.equals("<=")) {
					if (min <= comparisonNumber)
						context.write(key, new Text(Double.toString(min)));
				}
				else if (comparisonType.equals("=")) {
					if (min == comparisonNumber)
						context.write(key, new Text(Double.toString(min)));
				}
			}
			else if (aggFunc.equalsIgnoreCase("MAX")) {
				double max = 0.0;
				while (it.hasNext()) {
					double val = Double.parseDouble(it.next().toString());
					max = Math.max(max, val);
				}
				if (comparisonType.equals(">")) {
					if (max > comparisonNumber)
						context.write(key, new Text(Double.toString(max)));
				}
				else if (comparisonType.equals(">=")) {
					if (max >= comparisonNumber)
						context.write(key, new Text(Double.toString(max)));
				}
				else if (comparisonType.equals("<")) {
					if (max < comparisonNumber)
						context.write(key, new Text(Double.toString(max)));
				}
				else if (comparisonType.equals("<=")) {
					if (max <= comparisonNumber)
						context.write(key, new Text(Double.toString(max)));
				}
				else if (comparisonType.equals("=")) {
					if (max == comparisonNumber)
						context.write(key, new Text(Double.toString(max)));
				}
			}
			else if (aggFunc.equalsIgnoreCase("SUM")) {
				double sum = 0;
				while (it.hasNext()) {
					sum += Double.parseDouble(it.next().toString());
				}
				if (comparisonType.equals(">")) {
					if (sum > comparisonNumber)
						context.write(key, new Text(Double.toString(sum)));
				}
				else if (comparisonType.equals(">=")) {
					if (sum >= comparisonNumber)
						context.write(key, new Text(Double.toString(sum)));
				}
				else if (comparisonType.equals("<")) {
					if (sum < comparisonNumber)
						context.write(key, new Text(Double.toString(sum)));
				}
				else if (comparisonType.equals("<=")) {
					if (sum <= comparisonNumber)
						context.write(key, new Text(Double.toString(sum)));
				}
				else if (comparisonType.equals("=")) {
					if (sum == comparisonNumber)
						context.write(key, new Text(Double.toString(sum)));
				}
			}
			else if (aggFunc.equalsIgnoreCase("COUNT")) {
				long sum = 0;
				while (it.hasNext()) {
					sum += Double.parseDouble(it.next().toString());
				}
				if (comparisonType.equals(">")) {
					if (sum > comparisonNumber)
						context.write(key, new Text(Long.toString(sum)));
				}
				else if (comparisonType.equals(">=")) {
					if (sum >= comparisonNumber)
						context.write(key, new Text(Long.toString(sum)));
				}
				else if (comparisonType.equals("<")) {
					if (sum < comparisonNumber)
						context.write(key, new Text(Long.toString(sum)));
				}
				else if (comparisonType.equals("<=")) {
					if (sum <= comparisonNumber)
						context.write(key, new Text(Long.toString(sum)));
				}
				else if (comparisonType.equals("=")) {
					if (sum == comparisonNumber)
						context.write(key, new Text(Long.toString(sum)));
				}
			}
		}
	}

}
