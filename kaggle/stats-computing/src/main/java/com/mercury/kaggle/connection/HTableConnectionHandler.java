package com.mercury.kaggle.connection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


/**
 * Adaptation of Htable connector available here https://github.com/jrkinley/storm-hbase
 * 
 * HTable connector for Storm
 * <p>
 * The HBase configuration is picked up from the first <tt>hbase-site.xml</tt> encountered in the
 * classpath
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

public class HTableConnectionHandler {

    /**
     * HTable connected
     */
    protected HTable table;

    /**
     * Specific parameters for table connection
     */
    private HtableConfiguration htableConfiguration;


    private static final Logger LOG = LoggerFactory.getLogger(HTableConnectionHandler.class);

    /**
     * Initialize HTable connection
     * 
     * @param htableConfiguration The {@link HtableConfiguration}
     * @throws IOException
     */
    public HTableConnectionHandler(final HtableConfiguration htableConfiguration) {
        this.htableConfiguration = htableConfiguration;
    }

    /**
     * @return the table
     */
    public HTable getTable() {
        return table;
    }

    /**
     * Manage connection to table
     */
    public void connect() {
        String tableName = htableConfiguration.getTableName();
        // Build configuration
        Configuration conf = createConf();


        try {
            // Establish connection with the table
            this.table = new HTable(conf, tableName);
            LOG.debug(conf.toString());
        } catch (IOException e) {
            String errorMsg = "Unable to establish connection to HBase table " + tableName;
            LOG.error(errorMsg, e);
            throw new RuntimeException(errorMsg, e);
        }
        if (htableConfiguration.isBatch()) {
            // Enable client-side write buffer
            this.table.setAutoFlush(false, true);
            LOG.info("Enabled client-side write buffer");
        }
        // If set, override write buffer size
        if (0 < htableConfiguration.getWriteBufferSize()) {
            try {
                this.table.setWriteBufferSize(htableConfiguration.getWriteBufferSize());
                LOG.info("Setting client-side write buffer to "
                        + htableConfiguration.getWriteBufferSize());
            } catch (IOException ex) {
                LOG.error("Unable to set client-side write buffer size for HBase table "
                        + tableName, ex);
            }
        }
    }

    Configuration createConf() {
        Configuration conf = HBaseConfiguration.create();
        conf.addResource(this.getClass().getClassLoader().getResource("hbase-site.xml"));
        return conf;
    }

    /**
     * Manage the disconnection to hTable
     */
    public void disconnect() {
        try {
            if (table != null) {
                this.table.close();
            }
        } catch (IOException ex) {
            LOG.error(
                    "Unable to close connection to HBase table "
                            + htableConfiguration.getTableName(), ex);
        }

    }
}
