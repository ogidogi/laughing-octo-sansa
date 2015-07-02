package com.test.hbase.examples;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;

public class HBaseCreateTableIfNotExist {
    private static final Logger log = Logger.getLogger(HBaseCreateTableIfNotExist.class);

    public static void main(String[] args) throws ConfigurationException, IOException, DeserializationException {
        PropertiesConfiguration conf = new PropertiesConfiguration("hbase.properties");
        TableName tableName = TableName.valueOf(conf.getString("table.name"));
        String colFamily = conf.getString("column.family");

        log.setLevel(Level.DEBUG);
        log.debug("Create HBase connection");
        Connection conn = ConnectionFactory.createConnection();

        if (!conn.getAdmin().tableExists(tableName)) {
            log.debug(String.format("Table [%s] doesn't exist. Creating table...", tableName.getNameAsString()));
            HTableDescriptor desc = new HTableDescriptor(tableName);
            HColumnDescriptor family = new HColumnDescriptor(colFamily);

            desc.addFamily(family);
            conn.getAdmin().createTable(desc);

            log.debug(String.format("Table [%s] with column family [%s] created!", tableName.getNameAsString(),
                    colFamily));
        } else {
            log.debug(String.format("Table [%s] already exist. Exit", tableName.getNameAsString()));
            System.exit(0);
        }
    }
}
