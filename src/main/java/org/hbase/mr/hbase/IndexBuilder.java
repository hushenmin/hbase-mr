package org.hbase.mr.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

/**
 * Created by Administrator on 2018/2/11.
 */
public class IndexBuilder {
    static class Map extends TableMapper<ImmutableBytesWritable,Put>{
        private java.util.Map<byte[],ImmutableBytesWritable> indexs = new HashMap<byte[], ImmutableBytesWritable>();
        private  String famaily ;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //super.setup(context);
            Configuration conf = context.getConfiguration();
            String tableName = conf.get("tableName");
            famaily = conf.get("columnFamily");
            String[] qualifiers = conf.getStrings("qualifiers");
            for (String q : qualifiers){
                indexs.put(Bytes.toBytes(q),new ImmutableBytesWritable(Bytes.toBytes(tableName+"-"+q)));
            }

        }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            //super.map(key, value, context);
            Set<byte[]> keys = indexs.keySet();
            for (byte[] k:keys){
                ImmutableBytesWritable indexTableName = indexs.get(k);
                byte[] val = value.getValue(Bytes.toBytes(famaily),k);
                if (val != null){
                    Put put = new Put(val);
                    put.add(Bytes.toBytes("INDEX_CF"),Bytes.toBytes("id"),key.get());
                    context.write(indexTableName,put);
                }
            }
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
        if(otherArgs.length < 3){
            //IndexBuilder;TableName,Columnfamily,Qualifier
            System.exit(-1);
        }
        String tableName = args[0];
        String columnFamily= args[1];
        conf.set("tableName",tableName);
        conf.set("columnFamily",columnFamily);
        String[] qualifiers =  new String[otherArgs.length-2];
        for (int i =0; i<qualifiers.length;i++){
            qualifiers[i] = otherArgs[i+2];
        }
        conf.setStrings("qualifiers",qualifiers);
        Job job = new Job(conf,tableName);
        job.setJarByClass(IndexBuilder.class);
        job.setMapperClass(Map.class);
        job.setNumReduceTasks(0);
        job.setInputFormatClass(TableInputFormat.class);
        job.setOutputFormatClass(MultiTableOutputFormat.class);
        Scan scan = new Scan();
        TableMapReduceUtil.initTableMapperJob(tableName,scan,Map.class,ImmutableBytesWritable.class,Put.class,job);
        job.waitForCompletion(true);

    }
}
