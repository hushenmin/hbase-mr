package org.hbase.mr.hbase;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Admin;

import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by Administrator on 2018/2/9.
 */
public class SplitRegion {

    private static final Log LOG = LogFactory.getLog(SplitRegion.class);

    public static boolean createTable(Admin admin,
                                      HTableDescriptor table,
                                      byte[][] splits) {

        try {
            admin.createTable(table, splits);
            return true;
        } catch (IOException e) {
            LOG.info("table " + table.getTableName() + "already exists.");
            return false;
        }
    }
    public static byte[][] getHexSplits(String startKey,String endKey,int numRegions){
        byte[][] splits = new byte[numRegions -1][];
        BigInteger lowestKey = new BigInteger(startKey,16);
        BigInteger highestKey = new BigInteger(endKey,16);
        BigInteger range = highestKey.subtract(lowestKey);
        BigInteger regionIncrement = range.divide(BigInteger.valueOf(numRegions));
        lowestKey = lowestKey.add(regionIncrement);
        for (int i = 0; i < numRegions - 1; i++) {
            BigInteger key = lowestKey.add(regionIncrement.multiply(BigInteger.valueOf(i)));
            byte[] b = String.format("%016x",key).getBytes();
            splits[i] = b;
        }
         return splits;
    }


    private String makeRowKey(String id) {
        String md5_content = null;
        try {
            MessageDigest messageDigest = MessageDigest.getInstance("MD5");
            messageDigest.reset();
            messageDigest.update(id.getBytes());
            byte[] bytes = messageDigest.digest();
            md5_content = new String(Hex.encodeHex(bytes));
        } catch (NoSuchAlgorithmException e1) {
            e1.printStackTrace();
        }
        //turn right md5
        String right_md5_id = Integer.toHexString(Integer.parseInt(md5_content.substring(0, 7), 16) >> 1);
        while (right_md5_id.length() < 7) {
            right_md5_id = "0" + right_md5_id;
        }
        return right_md5_id + "::" + id;
    }


}
