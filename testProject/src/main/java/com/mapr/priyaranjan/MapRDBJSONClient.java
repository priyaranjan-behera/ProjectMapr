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

public static QueryCondition buildMultiplePinQueryCondition() {
	return MapRDB.newCondition()
			.and()
			.is("count", Op.GREATER, 1)
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




public static void getPinFilteredCityDataFromTable(String tablePath)
{
	// Reads the configurations from the conf folder as mentioned in the classpath. 
    
    //From the configuration we create a connection to the cluster. 
    try {

		System.out.println("Invoking function to get pin filtered city.");
		Table zipTable = MapRDB.getTable(tablePath+"_zips");
		DocumentStream documentStream = zipTable.find(buildMultiplePinQueryCondition());

		for(Document document : documentStream) {
			System.out.println("City with multiple Zips: " + document.getIdString());
		}
		
	} catch (Exception e) {
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
