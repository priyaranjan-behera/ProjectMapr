package com.mapr.priyaranjan;

import java.io.File;
import java.io.IOException;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class MapRJSONProcessing {
	
  @SuppressWarnings("deprecation")
public static void main(String[] args) throws IOException {
    
	  Scanner sc = new Scanner(new File("/tmp/zips.json"));
	  
	  while(sc.hasNext()){
	        System.out.println(sc.nextLine());
	  }
    
  }
}
