package com.mapr.priyaranjan;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.thrift.generated.Hbase.createTable_args;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import com.mapr.priyaranjan.MapRJSONProcessing;

public class MapRDBTabularClient {
	
	public static void createTable(String tableName)
	{
		try{

			// Reads the configurations from the conf folder as mentioned in the classpath. 
			Configuration config = HBaseConfiguration.create();
			
			// Lets create a HBaseAdmin here from the config
			HBaseAdmin admin = new HBaseAdmin(config);
			
			//creating table descriptor
			HTableDescriptor table = new HTableDescriptor(Bytes.toBytes("/tmp/java_table"));
	    	
	    	//creating column family descriptor
	    	HColumnDescriptor family = new HColumnDescriptor(Bytes.toBytes("column family"));
	
	    	//adding column family to HTable
	    	table.addFamily(family);
	    	
	    	admin.createTable(table);
	    	
		}catch(Exception e)
		{
			System.out.println("Error while creating table: " + e.getMessage());
			e.printStackTrace();
		}
		
	}
	
	
	public static void createRelTableforZip(String tableName)
	{
		try{

			// Reads the configurations from the conf folder as mentioned in the classpath. 
			Configuration config = HBaseConfiguration.create();
			
			// Lets create a HBaseAdmin here from the config
			HBaseAdmin admin = new HBaseAdmin(config);
			
			if(admin.tableExists(tableName))
			{
				System.out.println("Table already exists!");
				return;
			}
			
			//creating table descriptor
			HTableDescriptor table = new HTableDescriptor(Bytes.toBytes(tableName));
	    	
	    	//creating column family descriptor
	    	HColumnDescriptor family = new HColumnDescriptor(Bytes.toBytes("Identification"));
	    	//adding column family to HTable
	    	table.addFamily(family);
	    	
	    	//creating column family descriptor
	    	family = new HColumnDescriptor(Bytes.toBytes("Stats"));
	    	//adding column family to HTable
	    	table.addFamily(family);
	    	
	    	//creating column family descriptor
	    	family = new HColumnDescriptor(Bytes.toBytes("Location"));
	    	//adding column family to HTable
	    	table.addFamily(family);
	    	
	    	admin.createTable(table);
	    	
		}catch(Exception e)
		{
			System.out.println("Error while creating table: " + e.getMessage());
			e.printStackTrace();
		}
		
	}
	
	
	
	public static void addDataToTable(String tableName)
	{
		// Reads the configurations from the conf folder as mentioned in the classpath. 
	    Configuration config = HBaseConfiguration.create();
	    
	    //From the configuration we create a connection to the cluster. 
	    try {
			Connection connection = ConnectionFactory.createConnection(config);
			Table table = connection.getTable(TableName.valueOf("/tmp/java_table"));
			
			try {
				
				Put p = new Put(Bytes.toBytes("Row2"));
				p.add(Bytes.toBytes("column family"), Bytes.toBytes("column2"),Bytes.toBytes("12"));
				table.put(p);
				
			} catch (Exception e)
			{
				System.out.println("Error while writing to table: " + e.getMessage());
				e.printStackTrace();
			}finally {
				connection.close();
		    }
			
		} catch (IOException e) {
			System.out.println("Couldn't connect to the cluster: " + e.getMessage());
			e.printStackTrace();
		}
	    
	}
	
	public static void addDataToTableFromJSON(String fileName, String tableName)
	{
		// Reads the configurations from the conf folder as mentioned in the classpath. 
	    Configuration config = HBaseConfiguration.create();
	    
	    //From the configuration we create a connection to the cluster. 
	    try {
			Connection connection = ConnectionFactory.createConnection(config);
			Table table = connection.getTable(TableName.valueOf(tableName));
			List<JSONStructure> data = MapRJSONProcessing.getDataFromFile(fileName);
			
			try {
				for(JSONStructure row:data)
				{
					System.out.println("Adding zip to database: " + row.getId());
					Put p = new Put(Bytes.toBytes(row.getId()));
					p.add(Bytes.toBytes("Identification"), Bytes.toBytes("id"),Bytes.toBytes(row.getId()));
					table.put(p);
					
					p.add(Bytes.toBytes("Identification"), Bytes.toBytes("city"),Bytes.toBytes(row.getCity()));
					table.put(p);
					
					p.add(Bytes.toBytes("Identification"), Bytes.toBytes("state"),Bytes.toBytes(row.getState()));
					table.put(p);
					
					p.add(Bytes.toBytes("Stats"), Bytes.toBytes("pop"),Bytes.toBytes(row.getPop()));
					table.put(p);
					
					
					p.add(Bytes.toBytes("Location"), Bytes.toBytes("loc1"),Bytes.toBytes(row.getLocation().get(0).toString()));
					table.put(p);
					
					p.add(Bytes.toBytes("Location"), Bytes.toBytes("loc2"),Bytes.toBytes(row.getLocation().get(1).toString()));
					table.put(p);
				}
				
			} catch (Exception e)
			{
				System.out.println("Error while writing to table: " + e.getMessage());
				e.printStackTrace();
			}finally {
				connection.close();
		    }
			
		} catch (IOException e) {
			System.out.println("Couldn't connect to the cluster: " + e.getMessage());
			e.printStackTrace();
		}
	    
	}
	
	
	
	
	
	public static void getDataFromTable(String tableName)
	{
		// Reads the configurations from the conf folder as mentioned in the classpath. 
	    Configuration config = HBaseConfiguration.create();
	    
	    //From the configuration we create a connection to the cluster. 
	    try {
			Connection connection = ConnectionFactory.createConnection(config);
			Table table = connection.getTable(TableName.valueOf("/tmp/java_table"));
			
			try {
				
				Get g = new Get(Bytes.toBytes("Row1"));
				Result r = table.get(g);
				
				byte[] value = r.getValue(Bytes.toBytes("column family"),
				          Bytes.toBytes("column1"));
				
				System.out.println("Value retrieved is: " + Bytes.toString(value));
				
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
	
	
	
	public static void getAllDataFromTable(String tableName)
	{
		// Reads the configurations from the conf folder as mentioned in the classpath. 
	    Configuration config = HBaseConfiguration.create();
	    
	    //From the configuration we create a connection to the cluster. 
	    try {
			Connection connection = ConnectionFactory.createConnection(config);
			Table table = connection.getTable(TableName.valueOf("/tmp/java_table"));
			
			try {
				
				Scan s = new Scan();
		        s.addColumn(Bytes.toBytes("column family"), Bytes.toBytes("column1"));
		        ResultScanner scanner = table.getScanner(s);
		        
		        try {
		            // Scanners return Result instances.
		            for (Result rr : scanner) {
		            	
		            	byte[] value1 = rr.getValue(Bytes.toBytes("column family"),
						          Bytes.toBytes("column1"));
		            	byte[] value2 = rr.getValue(Bytes.toBytes("column family"),
						          Bytes.toBytes("column2"));
		            	System.out.println("Value1 retrieved is: " + Bytes.toString(value1));
		            	System.out.println("Value2 retrieved is: " + Bytes.toString(value2));
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
	
	
	
	public static void getAllZipDataFromTable(String tableName)
	{
		// Reads the configurations from the conf folder as mentioned in the classpath. 
	    Configuration config = HBaseConfiguration.create();
	    
	    //From the configuration we create a connection to the cluster. 
	    try {
			Connection connection = ConnectionFactory.createConnection(config);
			Table table = connection.getTable(TableName.valueOf(tableName));
			
			try {
				
				Scan s = new Scan();
		        s.addColumn(Bytes.toBytes("Identification"), Bytes.toBytes("id"));
		        s.addColumn(Bytes.toBytes("Identification"), Bytes.toBytes("city"));
		        s.addColumn(Bytes.toBytes("Identification"), Bytes.toBytes("state"));
		        s.addColumn(Bytes.toBytes("Stats"), Bytes.toBytes("pop"));
		        s.addColumn(Bytes.toBytes("Location"), Bytes.toBytes("loc1"));
		        s.addColumn(Bytes.toBytes("Location"), Bytes.toBytes("loc2"));
		        ResultScanner scanner = table.getScanner(s);
		        
		        try {
		            // Scanners return Result instances.
		            for (Result rr : scanner) {
		            	
		            	byte[] value1 = rr.getValue(Bytes.toBytes("Identification"),
						          Bytes.toBytes("id"));
		            	byte[] value2 = rr.getValue(Bytes.toBytes("Identification"),
						          Bytes.toBytes("city"));
		            	byte[] value6 = rr.getValue(Bytes.toBytes("Identification"),
						          Bytes.toBytes("state"));
		            	byte[] value3 = rr.getValue(Bytes.toBytes("Stats"),
						          Bytes.toBytes("pop"));
		            	byte[] value4 = rr.getValue(Bytes.toBytes("Location"),
						          Bytes.toBytes("loc1"));
		            	byte[] value5 = rr.getValue(Bytes.toBytes("Location"),
						          Bytes.toBytes("loc2"));
		            	System.out.println("*******************" + Bytes.toString(value1));
		            	System.out.println("Id retrieved is: " + Bytes.toString(value1));
		            	System.out.println("City retrieved is: " + Bytes.toString(value2));
		            	System.out.println("State retrieved is: " + Bytes.toString(value6));
		            	System.out.println("Pop retrieved is: " + Bytes.toDouble(value3));
		            	System.out.println("Loc1 retrieved is: " + Bytes.toString(value4));
		            	System.out.println("Loc2 retrieved is: " + Bytes.toString(value5));
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
    	//getAllDataFromTable("/tmp/java_table");
    	createRelTableforZip("/tmp/zips_rdb_table");
    	//System.out.println("Created Table");
    	addDataToTableFromJSON("/tmp/zips.json","/tmp/zips_rdb_table");
    	//System.out.println("Added Data to Table");
    	getAllZipDataFromTable("/tmp/zips_rdb_table");
    	System.out.println("Completed reading data from the table");
    	
     }
    finally {
       //connection.close();
     }
    
  }
}
