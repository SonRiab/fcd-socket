/*
 * Copyright (c) 2016. [j]karef GmbH
 */
package de.beuth_hochschule.spark.streaming;

import com.cloudera.sqlengine.aeprocessor.metadatautil.SqlTypes;
import de.beuth_hochschule.fcd.ExtendedFloatingCarData;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.sql.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Rene Jablonski <rene@vnull.de>
 * @date 12.11.16
 */
public class FcdToImpala {

    private static final Logger _LOG = LogManager.getLogger(FcdToImpala.class);
    private static final String JDBCDriver = "com.cloudera.impala.jdbc4.Driver";
    private static final String QUERY = "insert into extfcd values (?, ?, ?, ?, ?, ?, ?)";

    private static void printUsage() {
        System.out.println("Usage: FcdToImpala <hostname_port,...> <groupId> <topic> <partitions> <jdbc_url>");
        System.out.println("  <hostname_port,...>  a comma-separated list of zookeeper hostname:port urls");
        System.out.println("  <groupId>            the group id to use");
        System.out.println("  <topic>              the topic to consume from");
        System.out.println("  <partitions>         number of partitions to use");
        System.out.println("  <jdbc_url>           a valid jdbc url");
    }

    public static void main(String[] args) throws Exception {

        String zkQuorum = "";
        String groupId = "";
        String topic = "";
        String url = "";
//        final String jdbcUrl = "jdbc:impala://quickstart.cloudera:21050/test;AuthMech=0";
        int partitions = -1;

        if (args.length < 2) {
            printUsage();
            System.exit(1);
        }
        try {
            zkQuorum = args[0];
            groupId = args[1];
            topic = args[2];
            partitions = Integer.parseInt(args[3]);
            url = args[4];
        } catch (Exception e) {
            printUsage();
            System.exit(1);
        }
        final String jdbcUrl = url;

        Map<String, Integer> map = new HashMap<>();
        map.put(topic, partitions);

        SparkConf conf = new SparkConf().setAppName("FcdToImpala");
        JavaStreamingContext context = new JavaStreamingContext(conf, Durations.minutes(1));
        JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(context, zkQuorum, groupId, map);

        Class.forName(JDBCDriver);
        DriverManager.setLoginTimeout(30);

        final JavaDStream<ExtendedFloatingCarData> extFCDs = messages.map(new Function<Tuple2<String, String>, ExtendedFloatingCarData>() {
            @Override
            public ExtendedFloatingCarData call(Tuple2<String, String> tuple) throws Exception {
                return FcdMessageParser.parse(tuple._2());
            }
        });
        extFCDs.foreachRDD(new VoidFunction<JavaRDD<ExtendedFloatingCarData>>() {

            @Override
            public void call(JavaRDD<ExtendedFloatingCarData> extFCDJavaRDD) throws Exception {

                extFCDJavaRDD.foreachPartition(new VoidFunction<Iterator<ExtendedFloatingCarData>>() {
                    @Override
                    public void call(Iterator<ExtendedFloatingCarData> iterator) throws Exception {
                        ExtendedFloatingCarData extFCD;
                        int counter = 0;
                        Connection connection = DriverManager.getConnection(jdbcUrl);
                        connection.setAutoCommit(false);
                        PreparedStatement statement = connection.prepareStatement(QUERY);
                        try {
                            while (iterator.hasNext()) {
                                extFCD = iterator.next();

                                /* If the message parser couldn't parse a string, the result is null.
                                   We could not handle this type of error here and adding them to db makes no sense,
                                   so continue with next one.
                                 */
                                if(null == extFCD) {
                                    continue;
                                }

                                /* set all values to null */
                                statement.setNull(1, SqlTypes.SQL_VARCHAR.getSqlType());
                                statement.setNull(2, SqlTypes.SQL_DOUBLE.getSqlType());
                                statement.setNull(3, SqlTypes.SQL_DOUBLE.getSqlType());
                                statement.setNull(4, SqlTypes.SQL_FLOAT.getSqlType());
                                statement.setNull(5, SqlTypes.SQL_TIMESTAMP.getSqlType());
                                statement.setNull(6, SqlTypes.SQL_LONGVARCHAR.getSqlType());
                                /* only set values if not null */
                                if(null != extFCD.getId()) {
                                    statement.setString(1, extFCD.getId());
                                }
                                if(null != extFCD.getLongitude()) {
                                statement.setDouble(2, extFCD.getLongitude());
                                }
                                if(null != extFCD.getLatitude()) {
                                    statement.setDouble(3, extFCD.getLatitude());
                                }
                                if(null != extFCD.getSpeed()) {
                                    statement.setFloat(4, extFCD.getSpeed());
                                }
                                if(null != extFCD.getTimestamp()) {
                                    statement.setTimestamp(5, Timestamp.valueOf(extFCD.getTimestamp()));
                                }
                                if(null != extFCD.getExtendedData()) {
                                    statement.setString(6, extFCD.getExtendedData());
                                }
                                /* the dirty flag is always set, so it is save to set this value without checking
                                   its existence */
                                statement.setBoolean(7, extFCD.isDirty());
                                statement.addBatch();

                                /* execute batches of n inserts  */
                                if (counter++ % 1000 == 0) {
                                    statement.executeBatch();
                                    connection.commit();
                                    counter = 0;
                                }
                            }
                            /* execute the rest */
                            statement.executeBatch();
                            connection.commit();
                        } catch (SQLException e ) {
                            try {
                                _LOG.error("[foreachPartition] Transaction is being rolled back");
                                _LOG.trace(e);
                                connection.rollback();
                            } catch (SQLException excep) {
                                _LOG.error("[foreachPartition] Transaction is not rolled back");
                                _LOG.trace(excep);
                            }
                        } finally {
                            statement.close();
                            connection.close();
                        }
                    }
                });
            }
        });

        context.start();
        context.awaitTermination();
    }
}
