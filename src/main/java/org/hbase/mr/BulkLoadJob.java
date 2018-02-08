package org.hbase.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.LoggerFactory;

import org.slf4j.Logger;
import java.io.IOException;

public class BulkLoadJob {
    static Logger logger = LoggerFactory.getLogger(BulkLoadJob.class);
    public static class bulkLoadMap extends Mapper<LongWritable,Text,ImmutableBytesWritable,KeyValue> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] valueList = value.toString().split("\t");
            String hkey = valueList[0];
            String cf = valueList[1].split(":")[0];
            String cl = valueList[1].split(":")[1];
            String v = valueList[2];
            final byte[] rowKey = Bytes.toBytes(hkey);
            final ImmutableBytesWritable immutableBytesWritable
                    =new ImmutableBytesWritable(rowKey);
            KeyValue keyValue = new KeyValue(rowKey, Bytes.toBytes(cf), Bytes.toBytes(cl), Bytes.toBytes(v));

            context.write(immutableBytesWritable,keyValue);
            // super.map(key, value, context);
        }
    }

    public static void main(String[] args) {
        Configuration config = HBaseConfiguration.create();
        String input = args[0];
        String output =args[1];
        try {
            Job job = Job.getInstance(config, "BulkLoadMr");
            job.setJarByClass(BulkLoadJob.class);
            job.setMapperClass(BulkLoadJob.bulkLoadMap.class);
            job.setOutputKeyClass(ImmutableBytesWritable.class);
            job.setOutputValueClass(Put.class);
            job.setSpeculativeExecution(false);
            FileInputFormat.addInputPath(job,new Path(input));
            FileOutputFormat.setOutputPath(job,new Path(output));
            HTable hTable = new HTable(config,args[2]);
            HFileOutputFormat2.configureIncrementalLoad(job,hTable);
            try {
                if (job.waitForCompletion(true)){
                    FsShell fsShell = new FsShell(config);
                    fsShell.run(new String[]{"-chmod", "-R", "777", args[1]});
                    LoadIncrementalHFiles loadIncrementalHFiles = new LoadIncrementalHFiles(config);
                    loadIncrementalHFiles.doBulkLoad(new Path(output),hTable);
                }else {
                    logger.error("failed");
                    System.exit(-1);
                }
            } catch (Exception e) {
                logger.error("permission denied");
                e.printStackTrace();

            }finally {
                if(hTable !=null){
                    hTable.close();
                }
            }


        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
