package com.mapr.priyaranjan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.store.QueryCondition;
import org.ojai.store.QueryCondition.Op;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.mapr.db.MapRDB;
import com.mapr.db.Table;

public class MapRDBJSONClient {


	private static Table getDocTableforZipJSON(String tablePath) {
			if ( ! MapRDB.tableExists(tablePath)) {
				return MapRDB.createTable(tablePath);
		} else {
			return MapRDB.getTable(tablePath);
		}
	}
	
	
	private static Table getZipDocTableforZipJSON(String tablePath) {
		if ( ! MapRDB.tableExists(tablePath + "_zips")) {
			return MapRDB.createTable(tablePath + "_zips");
	} else {
		return MapRDB.getTable(tablePath + "_zips");
	}
}

public static void addDataToTableFromJSON(String fileName, String tableName)
{
	// Reads the configurations from the conf folder as mentioned in the classpath. 
	int count;
	int i=0;

	//From the configuration we create a connection to the cluster. 
	try {
		Table table = getDocTableforZipJSON(tableName);
		Table zipTable = getZipDocTableforZipJSON(tableName);
		
		List<Document> data = MapRJSONProcessing.getJSONDocsFromFile(fileName);
		
		Configuration config = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(config);

		org.apache.hadoop.hbase.client.Table stat_table = connection.getTable(TableName.valueOf(tableName+"_PinCount"));
		
		Set<String> cities = new HashSet<String>();

		try {
			for(Document row:data)
			{
				System.out.println("Adding to database, row: " + i++ + " city: " + row.getString("city"));
				table.insertOrReplace(row);
				cities.add(row.getString("city"));
				
				//Section to update the zip table
				
				Document tmpDoc;
				
				tmpDoc = zipTable.findById(row.getString("city"));
				Set<Object> zipSet;
				if(tmpDoc ==  null)
				{
					zipSet = new HashSet<Object>();
					zipSet.add(row.getString("_id"));
					count = 1;
					tmpDoc = MapRDB.newDocument().setId(row.getString("city")).set("zips",Lists.newArrayList(zipSet)).set("count", count);
					zipTable.insertOrReplace(tmpDoc);
					System.out.println("Inserting Count 1 for "+row.getString("city"));
				}
				else
				{
					zipSet = new HashSet<Object>(tmpDoc.getList("zips"));
					zipSet.add(row.getString("_id"));
					count  = zipSet.size();
					tmpDoc = MapRDB.newDocument().setId(row.getString("city")).set("zips",Lists.newArrayList(zipSet)).set("count", count);
					zipTable.insertOrReplace(tmpDoc);
					System.out.println("Inserting Count " + count + " for "+row.getString("city"));
				}

			}
			

		} catch (Exception e)
		{
			System.out.println("Error while writing to table: " + e.getMessage());
			e.printStackTrace();
		}finally {
			stat_table.close();
			connection.close();
		}

	} catch (Exception e) {
		System.out.println("Couldn't connect to the cluster: " + e.getMessage());
		e.printStackTrace();
	}

}

public static void findDocswithoutCondition(String tablePath) {

	try{
		Table table = getDocTableforZipJSON(tablePath);
		DocumentStream documentStream = table.find();
		for(Document document : documentStream) {
			System.out.println(document);
		}
	} catch(Exception e) {
		System.out.println("Error getting documents from the table");
		e.printStackTrace();
	}
}

public static QueryCondition buildQueryCondition() {
	return MapRDB.newCondition()
			.and()
			.is("city", Op.EQUAL, "SAN JOSE")
			.close()
			.build();
}

public static QueryCondition buildSanJoseQueryCondition(String tablePath) {
	
	Double totalPopulation = new Double(0);
	try{
		Table table = MapRDB.getTable(tablePath);
		DocumentStream documentStream = table.find(buildQueryCondition());
		

		for(Document document : documentStream) {
			System.out.println(document);
			totalPopulation = Double.sum(totalPopulation, document.getDouble("pop"));
		}
		
		System.out.println("Total San Jose Population: " + totalPopulation);
		

		
	}catch(Exception e) {
		System.out.println("Error creating query condition for San Jose Population");
		e.printStackTrace();
	}
	
	return MapRDB.newCondition()
			.and()
			.is("pop", Op.LESS, totalPopulation)
			.close()
			.build();
}

public static void findDocswithCondition(String tablePath, QueryCondition condition)
{
	try{
		Table table = MapRDB.getTable(tablePath);
		DocumentStream documentStream = table.find(condition);

		for(Document document : documentStream) {
			System.out.println(document);
		}
	}catch(Exception e) {
		System.out.println("Error getting documents from the table");
		e.printStackTrace();
	}
}

public static QueryCondition buildQueryConditionMultiplePins(String city) {
	return MapRDB.newCondition()
			.and()
			.is("city", Op.EQUAL, city)
			.close()
			.build();
}




public static int findNumDocswithPin(String tablePath, String city)
{
	int count = 0;
	try{
		Table table = MapRDB.getTable(tablePath);
		DocumentStream documentStream = table.find(buildQueryConditionMultiplePins(city));

		for(Document document : documentStream) {
			//System.out.println("Incrementing Count: " + count);
			count++;
		}
	}catch(Exception e) {
		System.out.println("Error getting documents from the table");
		e.printStackTrace();
	}

	return count;
}

public static void getPinFilteredCityDataFromTable(String tableName)
{
	// Reads the configurations from the conf folder as mentioned in the classpath. 
    Configuration config = HBaseConfiguration.create();
    
    //From the configuration we create a connection to the cluster. 
    try {
		Connection connection = ConnectionFactory.createConnection(config);
		org.apache.hadoop.hbase.client.Table table = connection.getTable(TableName.valueOf(tableName+"_PinCount"));
		
		System.out.println("Invoking function to get pin filtered city.");
		
		SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("Data"), Bytes.toBytes("pin"), CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(2));
		
		try {
			
			Scan s = new Scan();
	        s.addColumn(Bytes.toBytes("Data"), Bytes.toBytes("pin"));
	        s.addColumn(Bytes.toBytes("Data"), Bytes.toBytes("city"));
	        s.setFilter(filter);
	        ResultScanner scanner = table.getScanner(s);
	        
	        try {
	            // Scanners return Result instances.
	            for (Result rr : scanner) {
	            	
	            	byte[] value1 = rr.getValue(Bytes.toBytes("Data"),
					          Bytes.toBytes("city"));
	            	System.out.println("City with multiple zips: " + Bytes.toString(value1));
	            	
	            }
	          } finally {
	            // Make sure you close your scanners when you are done!
	            // Thats why we have it inside a try/finally clause
	            scanner.close();
	          }
			
			
		} catch (Exception e)
		{
			System.out.println("Error while reading from table: " + e.getMessage());
			e.printStackTrace();
		}finally {
			connection.close();
	    }
		
	} catch (IOException e) {
		// TODO Auto-generated catch block
		System.out.println("Couldn't connect to the cluster: " + e.getMessage());
		e.printStackTrace();
	}
    
    
}


@SuppressWarnings("deprecation")
public static void main(String[] args) throws IOException {

	try {
		String fileName = args[0];
    	String tableName = args[1];
    	
		addDataToTableFromJSON(fileName,tableName);
		findDocswithoutCondition(tableName);
		System.out.println("Completed reading data from the table");

		findDocswithCondition(tableName, buildSanJoseQueryCondition(tableName));
		
		System.out.println("Getting multiple zip cities:");
    	getPinFilteredCityDataFromTable(tableName);

	}
	finally {
		//connection.close();
	}

}
}
