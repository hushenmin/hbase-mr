package org.hbase.mr.hbase.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import javax.print.DocFlavor;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Administrator on 2018/2/11.
 */
public class testBufferedMutator extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(testBufferedMutator.class);
    private static final TableName TABLE_NAME = TableName.valueOf("MY_TABLE_NAME_TOO");
    private static final int POOL_SIZE = 10;
    private static final int TASK_COUNT = 100;
    private static final String CF = "DEFAULT_COLUMN_FAMILY";
    public AtomicLong count = new AtomicLong(0);
    private static final Configuration conf;
    private static final String ZK = "massive-dataset-new-002,massive-dataset-new-003,massive-dataset-new-004";

    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", ZK);
    }

    @Override
    public int run(String[] strings)  {
        BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener() {
            @Override
            public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator bufferedMutator) throws RetriesExhaustedWithDetailsException {
                for (int i = 0; i < e.getNumExceptions(); i++) {
                    LOG.info("failed sent to data"+e.getRow(i));
                }
            }
        };
        BufferedMutatorParams params = new BufferedMutatorParams(TABLE_NAME).listener(listener);

        ExecutorService workPool = Executors.newFixedThreadPool(POOL_SIZE);

        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(conf);
            final BufferedMutator mutator = connection.getBufferedMutator(params);

            List<Future<Void>> futures = new ArrayList<>(TASK_COUNT);
            for(int i =0;i < TASK_COUNT; i ++ ){
                futures.add(workPool.submit(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        Put put = new Put(Bytes.toBytes("row"));
                        put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes('q'), Bytes.toBytes("value"));
                        mutator.mutate(put);
                        return  null;

                    }
                }));

            }
            for (Future f:futures){
                f.get(5,TimeUnit.MINUTES);
            }
            workPool.shutdown();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }


        return 0;

    }

    public static void main(String[] args) {
        try {
            ToolRunner.run(new testBufferedMutator(),args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}


