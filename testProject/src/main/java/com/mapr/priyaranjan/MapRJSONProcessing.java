package com.mapr.priyaranjan;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

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
import org.ojai.Document;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import com.mapr.db.MapRDB;

public class MapRJSONProcessing {
	
  @SuppressWarnings("deprecation")
public static void main(String[] args) throws IOException {
    
	  List<JSONStructure> data = getDataFromFile("/tmp/zips.json");
	  System.out.println("Total number of data obtained is: " + data.size());
    
  }
  
  public static List<JSONStructure> getDataFromFile(String filename)
  {
	  List<JSONStructure> ret = new ArrayList<JSONStructure>();
	  try
	  {
		  Scanner sc = new Scanner(new File(filename));
	  
		  try
		  {
			 
		  String curJSON;
		  int i = 0;
		  JSONStructure currJSONStructure;
		  
		  while(sc.hasNext()){
			  	System.out.println("This is line: " + Integer.toString(i++));
		        curJSON = new String(sc.nextLine());
		        
		        Document document = MapRDB.newDocument(curJSON);
		        
		        System.out.println("Verifing if the document captured things:");
		        System.out.println("City: " + document.getString("city"));
		        System.out.println("State: " + document.getString("state"));
		        System.out.println("Pop: " + document.getDouble("pop"));
		        System.out.println("Id: " + document.getString("_id"));
		        System.out.println("loc: " + document.getList("loc").get(1));
		        
		        currJSONStructure = new JSONStructure(document.getString("city"), document.getString("state"), document.getDouble("pop"), document.getString("_id"), document.getList("loc"));
		        
		        ret.add(currJSONStructure);
		        
		  }
		  
		  }
		  catch(Exception e)
		  {
			  System.out.println("Error while reading from json: " + e.getMessage());
			  e.printStackTrace();
		  }
		  finally
		  {
			  sc.close();
		  }
	  
	  }
	  catch(Exception e)
	  {
		  System.out.println("Error creating the scanner for the give filename");
		  e.printStackTrace();
	  }
	  
	  return ret;
	  
  }



	public static List<Document> getJSONDocsFromFile(String filename)
	{
		  List<Document> ret = new ArrayList<Document>();
		  try
		  {
			  Scanner sc = new Scanner(new File(filename));
		  
			  try
			  {
				 
			  String curJSON;
			  int i = 0;
			  
			  while(sc.hasNext()){
				  	System.out.println("This is line: " + Integer.toString(i++));
			        curJSON = new String(sc.nextLine());
			        
			        Document document = MapRDB.newDocument(curJSON);
			        ret.add(document);
			        
			  }
			  
			  }
			  catch(Exception e)
			  {
				  System.out.println("Error while reading from json: " + e.getMessage());
				  e.printStackTrace();
			  }
			  finally
			  {
				  sc.close();
			  }
		  
		  }
		  catch(Exception e)
		  {
			  System.out.println("Error creating the scanner for the give filename");
			  e.printStackTrace();
		  }
		  
		  return ret;
		  
	}
}
