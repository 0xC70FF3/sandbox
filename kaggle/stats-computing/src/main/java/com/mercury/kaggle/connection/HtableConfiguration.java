package com.mercury.kaggle.connection;

import java.io.Serializable;

/**
 * Configuration for Hbase connection handler. Rewritten of TupleTableConfig class from storm-hbase
 * project
 */

/*
 * Original version:
 * 
 * Copyright 2012 James Kinley
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

public class HtableConfiguration implements Serializable {
    /**
	 * 
	 */
    private static final long serialVersionUID = -5139572683702698843L;

    private String tableName;

    private boolean batch;

    private boolean writeToWAL;

    private long writeBufferSize;

    /**
     * Initialize configuration
     * 
     * @param table The HBase table name
     */
    public HtableConfiguration(final String tableName) {
        this.tableName = tableName;
        this.batch = true;
        this.writeToWAL = true;
        this.writeBufferSize = 0L;
    }

    /**
     * Initialize configuration
     * 
     * @param table The HBase table name
     */
    public HtableConfiguration(final String tableName, boolean inBatchMode) {
        this.tableName = tableName;
        this.batch = inBatchMode;
        this.writeToWAL = true;
        this.writeBufferSize = 0L;
    }

    /**
     * @return the tableName
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * @return Whether batch mode is enabled
     */
    public boolean isBatch() {
        return batch;
    }

    /**
     * @param batch Whether to enable HBase's client-side write buffer.
     *        <p>
     *        When enabled your bolt will store put operations locally until the write buffer is
     *        full, so they can be sent to HBase in a single RPC call. When disabled each put
     *        operation is effectively an RPC and is sent straight to HBase. As your bolt can
     *        process thousands of values per second it is recommended that the write buffer is
     *        enabled.
     *        <p>
     *        Enabled by default
     */
    public void setBatch(boolean batch) {
        this.batch = batch;
    }

    /**
     * @param writeToWAL Sets whether to write to HBase's edit log.
     *        <p>
     *        Setting to false will mean fewer operations to perform when writing to HBase and hence
     *        better performance, but changes that haven't been flushed to a store file will be lost
     *        in the event of HBase failure
     *        <p>
     *        Enabled by default
     */
    public void setWriteToWAL(boolean writeToWAL) {
        this.writeToWAL = writeToWAL;
    }

    /**
     * @return True if write to HBase's edit log (WAL), false if not
     */
    public boolean isWriteToWAL() {
        return writeToWAL;
    }

    /**
     * @param writeBufferSize Overrides the client-side write buffer size.
     *        <p>
     *        By default the write buffer size is 2 MB (2097152 bytes). If you are storing larger
     *        data, you may want to consider increasing this value to allow your bolt to efficiently
     *        group together a larger number of records per RPC
     *        <p>
     *        Overrides the write buffer size you have set in your hbase-site.xml e.g.
     *        <code>hbase.client.write.buffer</code>
     */
    public void setWriteBufferSize(long writeBufferSize) {
        this.writeBufferSize = writeBufferSize;
    }

    /**
     * @return the writeBufferSize
     */
    public long getWriteBufferSize() {
        return writeBufferSize;
    }

}
