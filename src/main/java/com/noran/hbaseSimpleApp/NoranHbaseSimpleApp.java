package com.noran.hbaseSimpleApp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

public class NoranHbaseSimpleApp {


    public static void main(String[] args) throws IOException {
        HbaseOperations operations = new HbaseOperations();
        //Hbase configuration
        Configuration configuration = HBaseConfiguration.create();
        //Creating a table
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();
        Table table = connection.getTable(TableName.valueOf("People"));

        operations.createTable(admin);
        operations.putinATable(admin,table);
        operations.getDataFromATable(table);
        operations.scanATable(table);
        operations.deleteATable(admin, TableName.valueOf("People"));
    }
}
