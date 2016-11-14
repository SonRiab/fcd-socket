/*
 * Copyright (c) 2016. [j]karef GmbH
 */
package com.beuth_hochschule.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * @author Rene Jablonski <rene@vnull.de>
 * @date 12.11.16
 */
public class FcdSocket {

    private static final int MINIMAL_PORT = 1;
    private static final int MAXIMUM_PORT = 65535;

    public static void main(String[] args) throws Exception {

        String host = "";
        int port = -1;

        if (args.length < 2) {
            System.err.println("Usage: FcdSocket <hostname> <port>");
            System.exit(1);
        }
        try {
            host = args[0];
            port = Integer.parseInt(args[1]);
            if(MINIMAL_PORT < port && port < MAXIMUM_PORT) {
                throw new NumberFormatException("");
            }
        } catch (NumberFormatException e) {
            System.err.println(String.format("<port> have to be an integer value between %d and %d",
                    MINIMAL_PORT,
                    MAXIMUM_PORT));
            System.exit(1);
        }

        SparkConf conf = new SparkConf().setAppName("FcdSocket");
        JavaStreamingContext context = new JavaStreamingContext(conf, Durations.seconds(5));
        JavaReceiverInputDStream<String> lines = context.socketTextStream(host, port);

        lines.map(FcdMessageParser::parse);
    }
}
