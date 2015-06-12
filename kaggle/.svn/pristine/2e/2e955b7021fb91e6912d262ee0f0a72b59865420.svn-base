package com.mercury.kaggle.connection;

import com.mercury.kaggle.constants.ColumnNameConstants;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.UUID;

public class Plaf implements ColumnNameConstants {

    /**
     * ssh root@namenode1.keyade.pro // hbase shell // create 'kaggle-train', {NAME => 'd'}
     */

    static class LineWorker {
        private HTableConnectionHandler connection;
        private boolean train;

        LineWorker(HTableConnectionHandler connection, boolean train) {
            this.connection = connection;
            this.train = train;
        }

        void work(String line) throws Exception {
            int offset = (this.train) ? 1 : 0;
            String[] exps = (line + " ").split(",");
            
            UUID uuid = UUID.randomUUID();
            ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
            bb.putLong(uuid.getMostSignificantBits());
            bb.putLong(uuid.getLeastSignificantBits());
            
            Put put = new Put(bb.array());
            this.putLong(put, ColumnNameConstants.ID, exps[0]);

            if (this.train) {
                put.add(ColumnNameConstants.DATA_COLUMN_FAMILY, ColumnNameConstants.LABEL, Bytes.toBytes("1".equals(exps[1])));
            }

            this.putLong(put, ColumnNameConstants.I1, exps[1 + offset]);
            this.putLong(put, ColumnNameConstants.I2, exps[2 + offset]);
            this.putLong(put, ColumnNameConstants.I3, exps[3 + offset]);
            this.putLong(put, ColumnNameConstants.I4, exps[4 + offset]);
            this.putLong(put, ColumnNameConstants.I5, exps[5 + offset]);
            this.putLong(put, ColumnNameConstants.I6, exps[6 + offset]);
            this.putLong(put, ColumnNameConstants.I7, exps[7 + offset]);
            this.putLong(put, ColumnNameConstants.I8, exps[8 + offset]);
            this.putLong(put, ColumnNameConstants.I9, exps[9 + offset]);
            this.putLong(put, ColumnNameConstants.I10, exps[10 + offset]);
            this.putLong(put, ColumnNameConstants.I11, exps[11 + offset]);
            this.putLong(put, ColumnNameConstants.I12, exps[12 + offset]);
            this.putLong(put, ColumnNameConstants.I13, exps[13 + offset]);

            offset += 13;
            
            this.putHexadecimal(put, ColumnNameConstants.C1, exps[1 + offset]);
            this.putHexadecimal(put, ColumnNameConstants.C2, exps[2 + offset]);
            this.putHexadecimal(put, ColumnNameConstants.C3, exps[3 + offset]);
            this.putHexadecimal(put, ColumnNameConstants.C4, exps[4 + offset]);
            this.putHexadecimal(put, ColumnNameConstants.C5, exps[5 + offset]);
            this.putHexadecimal(put, ColumnNameConstants.C6, exps[6 + offset]);
            this.putHexadecimal(put, ColumnNameConstants.C7, exps[7 + offset]);
            this.putHexadecimal(put, ColumnNameConstants.C8, exps[8 + offset]);
            this.putHexadecimal(put, ColumnNameConstants.C9, exps[9 + offset]);
            this.putHexadecimal(put, ColumnNameConstants.C10, exps[10 + offset]);
            this.putHexadecimal(put, ColumnNameConstants.C11, exps[11 + offset]);
            this.putHexadecimal(put, ColumnNameConstants.C12, exps[12 + offset]);
            this.putHexadecimal(put, ColumnNameConstants.C13, exps[13 + offset]);
            this.putHexadecimal(put, ColumnNameConstants.C14, exps[14 + offset]);
            this.putHexadecimal(put, ColumnNameConstants.C15, exps[15 + offset]);
            this.putHexadecimal(put, ColumnNameConstants.C16, exps[16 + offset]);
            this.putHexadecimal(put, ColumnNameConstants.C17, exps[17 + offset]);
            this.putHexadecimal(put, ColumnNameConstants.C18, exps[18 + offset]);
            this.putHexadecimal(put, ColumnNameConstants.C19, exps[19 + offset]);
            this.putHexadecimal(put, ColumnNameConstants.C20, exps[20 + offset]);
            this.putHexadecimal(put, ColumnNameConstants.C21, exps[21 + offset]);
            this.putHexadecimal(put, ColumnNameConstants.C22, exps[22 + offset]);
            this.putHexadecimal(put, ColumnNameConstants.C23, exps[23 + offset]);
            this.putHexadecimal(put, ColumnNameConstants.C24, exps[24 + offset]);
            this.putHexadecimal(put, ColumnNameConstants.C25, exps[25 + offset]);
            this.putHexadecimal(put, ColumnNameConstants.C26, exps[26 + offset].trim());

            connection.getTable().put(put);
        }
        
        
        public void putLong(Put put, byte[] qualifier, String exp) {
            if (exp != null && !exp.isEmpty()) {
                put.add(ColumnNameConstants.DATA_COLUMN_FAMILY, qualifier, Bytes.toBytes(Long.parseLong(exp)));
            }            
        }
        
        public void putHexadecimal(Put put, byte[] qualifier, String exp) {
            if (exp != null && !exp.isEmpty()) {
                put.add(ColumnNameConstants.DATA_COLUMN_FAMILY, qualifier, Bytes.toBytes((int)Long.parseLong(exp, 16)));
            }            
        }
    };

    

    public static void readFile(File file, LineWorker worker) throws Exception {
        LineIterator iterator = null;
        try {
            iterator = FileUtils.lineIterator(file, "UTF-8");
            if (iterator.hasNext()) {
                iterator.nextLine(); //skip header line
                int i = 1;
                while (iterator.hasNext()) {
                    worker.work(iterator.nextLine());
                    System.out.print( i++ + "\r");
                }
                System.out.print("\n");
            }            
        } finally {
            if (iterator != null) {
                LineIterator.closeQuietly(iterator);
            }
        }
    }
    
    public static void main(String... args) throws Exception {
        HTableConnectionHandler t = null;
        try {
            t = new HTableConnectionHandler(new HtableConfiguration("kaggle-train", true));
            t.connect();
            readFile(new File("/home/matthieu/Bureau/train.csv"), new LineWorker(t, true));
            t.getTable().flushCommits();
        } finally {
            if (t != null) {                
                t.disconnect();
            }
        }
    }

}
