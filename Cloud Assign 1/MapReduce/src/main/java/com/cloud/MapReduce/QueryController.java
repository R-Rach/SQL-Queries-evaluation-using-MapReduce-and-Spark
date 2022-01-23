package com.cloud.MapReduce;

import java.io.IOException;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.cloud.MapReduce.Jobs.MapReduceJob;
import com.cloud.MapReduce.Jobs.SparkJob;
import com.cloud.MapReduce.Model.InputModel;
import com.cloud.MapReduce.Model.OutputModel;
import com.cloud.MapReduce.Model.QueryModel;
import com.cloud.MapReduce.SQL.SqlQueryParser;

@Path("/query")
public class QueryController {

	/**
	 * Method called through API to Map-Reduce and Spark Job
	 * 
	 * @param input an instance of {@link InputModel.java}
	 * @return an instance of {@link OutputModel.java} having relevant fields for
	 *         output
	 * @throws ClassNotFoundException
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws IllegalArgumentException
	 */
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public OutputModel executeQuery(InputModel input)
			throws ClassNotFoundException, IOException, InterruptedException, IllegalArgumentException {

		String query = input.getQuery();
		QueryModel parsedQuery = new QueryModel(query);
		SqlQueryParser.parse(parsedQuery);

		// print tokens of query on console
		System.out.println(parsedQuery);

		// execute map-reduce job
		OutputModel output = MapReduceJob.executeMapReduce(parsedQuery);
		System.out.println("==============MAP REDUCE DONE===============");

		// execute spark job
		System.out.println("==============SPARK STARTED===============");
		SparkJob.execute(parsedQuery, output);
		
		return output;

	}

	/**
	 * Method designed just for testing MAP-REDUCE and SPARK without API
	 * 
	 * @param args
	 * @throws ClassNotFoundException
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws IllegalArgumentException
	 */
	public static void main(String[] args)
			throws ClassNotFoundException, IOException, InterruptedException, IllegalArgumentException {
		
		// reviews table queries
		String query1 = "SELECT customerid, count(*) FROM reviews WHERE rating = 5 GROUP BY customerid HAVING count(*) >= 1";//W //>=50 5351 lines for whole data
		String query2 = "SELECT id, customerid, count(*) FROM reviews WHERE rating = 5 GROUP BY id, customerid HAVING count(*) >= 2";//W
		String query3 = "SELECT customerid, sum(votes) FROM reviews WHERE rating = 5 GROUP BY customerid HAVING sum(votes) >= 50";//W
		String query4 = "SELECT id, customerid, max(votes) FROM reviews WHERE rating = 5 GROUP BY id, customerid HAVING max(votes) > 0";//W
		String query5 = "SELECT customerid, sum(helpful) FROM reviews WHERE rating = 5 GROUP BY customerid HAVING sum(helpful) >= 50";//W
		String query6 = "SELECT customerid, sum(helpful) FROM reviews WHERE customerid = \"A3UN6WX5RRO2AG\" GROUP BY customerid HAVING sum(helpful) >= 50";//W
		String query7 = "SELECT id, customerid, min(votes) FROM reviews WHERE rating = 5 GROUP BY id, customerid HAVING min(votes) = 0";
		String query8 = "SELECT id, customerid, max(votes) FROM reviews WHERE rating = 5 GROUP BY id, customerid HAVING max(votes) > 0";
		
		// similar table queries
		String query9 = "SELECT id, count(*) FROM similar WHERE id = 1 GROUP BY id HAVING count(*) >= 1";//W
		String query10 = "SELECT id, asin, count(*) FROM similar WHERE id = 1 GROUP BY id, asin HAVING count(*) >= 1";//W
		String query11 = "SELECT id, similarasin, count(*) FROM similar WHERE id = 1 GROUP BY id, similarasin HAVING count(*) >= 1";//W
		String query12 = "SELECT id, asin, similarasin, count(*) FROM similar WHERE similarasin = \"157794349X\" GROUP BY id, asin, similarasin HAVING count(*) >= 1";//W
		String query13 = "SELECT asin, sum(id) FROM similar WHERE asin = \"0738700797\" GROUP BY asin HAVING sum(id) > 0";
		
		// products table queries
		String query14 = "SELECT id, count(*) FROM products WHERE group = \"Book\" GROUP BY id HAVING count(*) >= 1";//W
		String query15 = "SELECT avgrating, count(*) FROM products WHERE avgrating = 5 GROUP BY avgrating HAVING count(*) >= 1";//W
		String query16 = "SELECT id, asin, salesrank count(*) FROM products WHERE title = \"Fantastic Food with Splenda : 160 Great Recipes for Meals Low in Sugar, Carbohydrates, Fat, and Calories\" GROUP BY id, asin, salesrank HAVING count(*) >= 1";
		String query17 = "SELECT avgrating, sum(avgrating) FROM products WHERE avgrating = 3.5 GROUP BY avgrating HAVING sum(avgrating) > 0";
		// (put all combi of > < >= <= = in HAVING clause)
		String query18 = "SELECT avgrating, max(nosimilar) FROM products WHERE group = \"Book\" GROUP BY avgrating HAVING max(nosimilar) > 1";
		String query19 = "SELECT avgrating, sum(avgrating) FROM products WHERE group = \"Book\" GROUP BY avgrating HAVING sum(avgrating) <= 21";
		String query20 = "SELECT avgrating, min(downloaded) FROM products WHERE group = \"Book\" GROUP BY avgrating HAVING min(downloaded) > 1";
		String query21 = "SELECT avgrating, count(*) FROM products WHERE group = \"Book\" GROUP BY avgrating HAVING count(*) >= 1";
	
		///// if there is a ' in title... then put \\ before it in query for SPARK ONLY
		
		// categories table queries
		String query22 = "SELECT category, count(*) FROM categories WHERE category = \"Religion & Spirituality[22]\" GROUP BY category HAVING count(*) >= 1";
		
		InputModel in = new InputModel();
		in.setQuery(query22);

		QueryController obj = new QueryController();

		OutputModel out = obj.executeQuery(in);

		System.out.println("================================");
		System.out.println(out);
		System.out.println("================================");
		System.out.println("done!!");

	}

	//////////////////////////////////////
	/**
	 * API TESTING METHODS
	 */

	@GET
	@Produces(MediaType.TEXT_PLAIN)
	@Path("/test1")
	public String testMsg() {
		return "Hello!!!";
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/test2")
	public OutputModel testJson() {
		OutputModel o = new OutputModel();
		o.setMapReduceExecutionTime("xxhadoopExecutionTime");
		o.setMapperInput("xxmapperInput");
		o.setMapperOutput("xxmapperOutput");
		o.setReducerInput("xxreducerInput");
		o.setReducerOutput("xxreducerOutput");
		o.setSparkExecutionTime("xxsparkExecutionTime");
		o.setSparkTransformations("xxsparkTransformations");
		return o;
	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/test3")
	public QueryModel testPost(InputModel input) {

		String query = input.getQuery();
		QueryModel parsedQuery = new QueryModel(query);
		SqlQueryParser.parse(parsedQuery);
		System.out.println(parsedQuery);
		return parsedQuery;
	}

}
