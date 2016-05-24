package com.mapr.priyaranjan;

import java.io.IOException;
import java.util.List;

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

import com.mapr.db.MapRDB;
import com.mapr.db.Table;

public class MapRDBJSONClient {


	private static Table getDocTableforZipJSON(String tablePath) {
		if ( ! MapRDB.tableExists(tablePath)) {

			try{

				// Reads the configurations from the conf folder as mentioned in the classpath. 
				Configuration config = HBaseConfiguration.create();

				// Lets create a HBaseAdmin here from the config
				HBaseAdmin admin = new HBaseAdmin(config);

				if(admin.tableExists(tablePath+"_PinCount"))
				{
					System.out.println("Table already exists!");
				}
				else
				{
					HTableDescriptor table = new HTableDescriptor(Bytes.toBytes(tablePath+"_PinCount"));
					HColumnDescriptor family = new HColumnDescriptor(Bytes.toBytes("Data"));
					table.addFamily(family);
					admin.createTable(table);
				}
			}catch(Exception e)
			{
				System.out.println("Error while creating stats table: " + e.getMessage());
				e.printStackTrace();
			}


			return MapRDB.createTable(tablePath);
	} else {
		return MapRDB.getTable(tablePath);
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
		List<Document> data = MapRJSONProcessing.getJSONDocsFromFile(fileName);
		
		Configuration config = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(config);

		try {
			for(Document row:data)
			{
				System.out.println("Adding to database, row: " + i++);
				table.insertOrReplace(row);
				
				Get g = new Get(Bytes.toBytes(row.getString("city")));
				System.out.println("Adding to database, city: " + row.getString("city"));

				org.apache.hadoop.hbase.client.Table stat_table = connection.getTable(TableName.valueOf(tableName+"_PinCount"));
				if(!stat_table.exists(g))
				{
					Put p = new Put(Bytes.toBytes(row.getString("city")));
					p.add(Bytes.toBytes("Data"), Bytes.toBytes("pin"),Bytes.toBytes(1));
					stat_table.put(p);
					p.add(Bytes.toBytes("Data"), Bytes.toBytes("city"),Bytes.toBytes(row.getString("city")));
					stat_table.put(p);
				}
				else
				{
					count = findNumDocswithPin(tableName, row.getString("city"));
					System.out.println("Count found: " + count);
					
			        if(count > 1)
			        {
			        	Put p = new Put(Bytes.toBytes(row.getString("city")));
						p.add(Bytes.toBytes("Data"), Bytes.toBytes("pin"),Bytes.toBytes(count));
						stat_table.put(p);
						p.add(Bytes.toBytes("Data"), Bytes.toBytes("city"),Bytes.toBytes(row.getString("city")));
						stat_table.put(p);
			        }
				}
				
				
				stat_table.close();
				
				
			}

		} catch (Exception e)
		{
			System.out.println("Error while writing to table: " + e.getMessage());
			e.printStackTrace();
		}finally {
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
			System.out.println("Incrementing Count: " + count);
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
		addDataToTableFromJSON("/tmp/zips.json","/tmp/zips_json2_table");
		findDocswithoutCondition("/tmp/zips_json2_table");
		System.out.println("Completed reading data from the table");

		findDocswithCondition("/tmp/zips_json2_table", buildQueryCondition());
		
		System.out.println("Getting multiple zip cities:");
    	getPinFilteredCityDataFromTable("/tmp/zips_json2_table");

	}
	finally {
		//connection.close();
	}

}
}
