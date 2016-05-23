package com.mapr.priyaranjan;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.ojai.Document;

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
	
	public static void addDataToTableFromJSON(String fileName, String tableName)
	{
		// Reads the configurations from the conf folder as mentioned in the classpath. 
	    Configuration config = HBaseConfiguration.create();
	    
	    //From the configuration we create a connection to the cluster. 
	    try {
			Table table = getDocTableforZipJSON(tableName);
			List<Document> data = MapRJSONProcessing.getJSONDocsFromFile(fileName);
			
			try {
				for(Document row:data)
				{
					table.insertOrReplace(row);
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
	
	
	
	
  @SuppressWarnings("deprecation")
public static void main(String[] args) throws IOException {
    
    try {
    	//getAllDataFromTable("/tmp/java_table");
    	//createTableforZipJSON("/tmp/zips_table");
    	//System.out.println("Created Table");
    	//addDataToTableFromJSON("/tmp/zips.json","/tmp/zips_table");
    	//System.out.println("Added Data to Table");
    	addDataToTableFromJSON("/tmp/zips.json","/tmp/zips_json_table");
    	System.out.println("Completed reading data from the table");
    	
     }
    finally {
       //connection.close();
     }
    
  }
}
