package tzu;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;

public class HbaseClient {
	public static void main(String[] args) throws IOException {
		Configuration config = HBaseConfiguration.create();
		// Create table
		HBaseAdmin admin = new HBaseAdmin(config);
		HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("??choose_a_name??"));
		HColumnDescriptor hcd = new HColumnDescriptor("data");
		htd.addFamily(hcd);
		admin.createTable(htd);
		byte [] tablename = htd.getName();
		HTableDescriptor [] tables = admin.listTables();
		if (tables.length != 1 && Bytes.equals(tablename, tables[0].getName())) {
			throw new IOException("Failed create of table");
		}
		
		// Run some operations -- a put, and a get
		HTable table = new HTable(config, tablename);
		byte [] row1 = Bytes.toBytes("row1");
		Put p1 = new Put(row1);
		byte [] databytes = Bytes.toBytes("data");
		p1.add(databytes, Bytes.toBytes("col1"), Bytes.toBytes("value1"));
		p1.add(databytes, Bytes.toBytes("col2"), Bytes.toBytes("value2"));
		table.put(p1);
		
		Get g = new Get(row1);
		Result result = table.get(g);
		// ??
		// fix the output format to the following format
		// rowKey,  columnFamily:qualifier, value
		System.out.println("Get: " + result);
		
		// Let's put more data the table
		byte [] row2 = Bytes.toBytes("row2");
		Put p2 = new Put(row2);
		p2.add(databytes, Bytes.toBytes("col1"), Bytes.toBytes("value3"));
		table.put(p2);
		
		byte [] row3 = Bytes.toBytes("row3");
		Put p3 = new Put(row3);
		p3.add(databytes, Bytes.toBytes("col2"), Bytes.toBytes("value4"));
		table.put(p3);
		
		// ??
		// Write a scan on whole table.
		// Put your code here
		// ...
		
		// Drop the table
		admin.disableTable(tablename);
		admin.deleteTable(tablename);
	}

}
