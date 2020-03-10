package com.noran.hbaseSimpleApp;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;

public class HbaseOperations {

    private void configureHabse(){ }

    void createTable(Admin admin) throws IOException {
        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf("People"));
        descriptor.addFamily(new HColumnDescriptor("PersonalInfo"));
        descriptor.addFamily(new HColumnDescriptor("CreditInfo"));
        admin.createTable(descriptor);
    }

    void putinATable(Admin admin, Table table) throws IOException {
        //Put data in table
        Put put1 = new Put(Bytes.toBytes("Row1"));
        put1.addImmutable(Bytes.toBytes("PersonalInfo"), Bytes.toBytes("FirstName"), Bytes.toBytes("Nora"));
        put1.addImmutable(Bytes.toBytes("PersonalInfo"), Bytes.toBytes("LastName"), Bytes.toBytes("N"));
        put1.addImmutable(Bytes.toBytes("CreditInfo"), Bytes.toBytes("CreditCardNo"), Bytes.toBytes("0000000"));
        table.put(put1);

        Put put2 = new Put(Bytes.toBytes("Row2"));
        put2.addImmutable(Bytes.toBytes("PersonalInfo"), Bytes.toBytes("FirstName"), Bytes.toBytes("Mammad"));
        put2.addImmutable(Bytes.toBytes("PersonalInfo"), Bytes.toBytes("LastName"), Bytes.toBytes("A"));
        put2.addImmutable(Bytes.toBytes("CreditInfo"), Bytes.toBytes("CreditCardNo"), Bytes.toBytes("111111"));
        table.put(put2);
    }

    void getDataFromATable(Table table) throws IOException {
        //Get data from a table
        Get get = new Get(Bytes.toBytes("Row1"));
        Result result = table.get(get);
    }

    void scanATable(Table table) throws IOException {
        //Scan a table for certain data
        Scan scan = new Scan(Bytes.toBytes("Row1"));
        scan.addColumn(Bytes.toBytes("PersonalInfo"), Bytes.toBytes("FirstName"));
        scan.addColumn(Bytes.toBytes("PersonalInfo"), Bytes.toBytes("LastName"));
        scan.addColumn(Bytes.toBytes("CreditInfo"), Bytes.toBytes("CreditCardNo"));

        try(ResultScanner scanner = table.getScanner(scan)) {
            for (Result result : scanner) {
                for (Cell cell : result.listCells()) {
                    System.out.println("Qualifier: "+Bytes.toString(CellUtil.cloneQualifier(cell))
                            +" Value: " + Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
        }
    }
    void deleteATable(Admin admin, TableName tableName) throws IOException {
        //Delete a table
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
    }
}
