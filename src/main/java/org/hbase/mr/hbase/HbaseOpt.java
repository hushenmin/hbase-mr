package org.hbase.mr.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression;

import java.io.IOException;

/**
 * Created by Administrator on 2018/2/8.
 */
public class HbaseOpt {
    private static  final Configuration conf;
    private static  final  String ZK = "massive-dataset-new-002,massive-dataset-new-003,massive-dataset-new-004";
    private static final String TABLE_NAME = "MY_TABLE_NAME_TOO";
    private static final String CF_DEFAULT = "DEFAULT_COLUMN_FAMILY";
    static{
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", ZK);
    }
    public static  void  createOrOverwrite(Admin admin,HTableDescriptor descriptor) throws IOException {
        if (admin.tableExists(descriptor.getTableName())){
            admin.disableTable(descriptor.getTableName());
            admin.deleteTable(descriptor.getTableName());
        }
        admin.createTable(descriptor);

    }

    public static void createSchemaTables(Configuration conf){
        Connection connection;
        try {
            connection= ConnectionFactory.createConnection(conf);
            Admin admin = connection.getAdmin();
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            descriptor.addFamily(new HColumnDescriptor(CF_DEFAULT).setCompressionType(Compression.Algorithm.SNAPPY));
            System.out.println("Creating table ......");
            createOrOverwrite(admin,descriptor);
            } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void modifySchema(Configuration conf){
        try {
            Connection connection = ConnectionFactory.createConnection();
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(TABLE_NAME);
            if(!admin.tableExists(tableName)){
                System.out.println("Table does not exist ...");
                System.exit(-1);
            }
            HTableDescriptor descriptor = admin.getTableDescriptor(tableName);
            HColumnDescriptor columnDescriptor  = new HColumnDescriptor("NEWCF");
            columnDescriptor.setCompactionCompressionType(Compression.Algorithm.GZ);
            columnDescriptor.setMaxVersions(HConstants.ALL_VERSIONS);
            descriptor.modifyFamily(columnDescriptor);
            admin.modifyTable(tableName,descriptor);
            admin.disableTable(tableName);
            admin.deleteColumn(tableName,CF_DEFAULT.getBytes("UTF-8"));
            admin.disableTable(tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    public static void main(String[] args) {
        createSchemaTables(conf);
        //modifySchema(conf);
    }
}
