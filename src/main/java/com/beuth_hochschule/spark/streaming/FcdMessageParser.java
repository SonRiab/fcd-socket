/*
 * Copyright (c) 2016. [j]karef GmbH
 */
package com.beuth_hochschule.spark.streaming;

import de.beuth_hochschule.fcd.ExtendedFloatingCarData;
import de.beuth_hochschule.fcd.ExtendedFloatingCarDataImpl;
import de.beuth_hochschule.fcd.taxi.BusyState;
import de.beuth_hochschule.fcd.taxi.TaxiFloatingCarDataImpl;
import de.beuth_hochschule.fcd.taxi.WaitingState;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.StringTokenizer;

/**
 * @author Rene Jablonski <rene@vnull.de>
 * @date 14.11.16
 */
public class FcdMessageParser {

    public static ExtendedFloatingCarData parse(String line) {
        ExtendedFloatingCarData fcd = null;
        float speed, longitude, latitude;
        OffsetDateTime timestamp;
        String extendedData = "";

        StringTokenizer tokenizer = new StringTokenizer(line, ",");
        ArrayList<String> tokens = new ArrayList<>(tokenizer.countTokens());
        while(tokenizer.hasMoreTokens()) {
            tokens.add(tokenizer.nextToken());
        }
        switch (tokenizer.countTokens()) {
            case 10:
                try {
                    float degree;
                    int gpsState;
                    WaitingState waitingState;
                    BusyState busyState;

                    timestamp = OffsetDateTime.parse(tokens.get(2));
                    longitude = Float.parseFloat(tokens.get(3));
                    latitude = Float.parseFloat(tokens.get(4));
                    waitingState = getWaitingState(Integer.parseInt(tokens.get(5)));
                    busyState = getBusyState(Integer.parseInt(tokens.get(6)));
                    degree = Float.parseFloat(tokens.get(7));
                    speed = Float.parseFloat(tokens.get(8));
                    gpsState = Integer.parseInt(tokens.get(9));

                    fcd = new TaxiFloatingCarDataImpl(tokens.get(0), tokens.get(1), timestamp, longitude, latitude,
                            waitingState, busyState, degree, speed, gpsState, "");
                } catch (Exception e) {
                    // TODO
                }
                if(null == fcd) {
                    System.err.println("Could not parse the following line: " + line);
                }
                break;
            default:
                try {
                    speed = Float.parseFloat(tokens.get(1));
                    timestamp = OffsetDateTime.parse(tokens.get(2));
                    longitude = Float.parseFloat(tokens.get(3));
                    latitude = Float.parseFloat(tokens.get(4));
                    if(tokens.size() > 5) {
                        for(String token : tokens.subList(5, tokens.size() - 1)) {
                            extendedData += token + ",";
                        }
                        // remove last comma
                        extendedData = extendedData.substring(0, extendedData.length() - 1);
                    }

                    fcd = new ExtendedFloatingCarDataImpl(tokens.get(0), speed, timestamp, longitude, latitude,
                            extendedData);
                } catch (Exception e) {
                    // TODO
                }
                if(null == fcd) {
                    System.err.println("Could not parse the following line: " + line);
                }
        }
        return fcd;
    }

    private static WaitingState getWaitingState(int state) {
        if (state == 1)
            return WaitingState.DRIVING;
        else
            return WaitingState.WAITING;
    }

    private static BusyState getBusyState(int state) {
        if (state == 1)
            return BusyState.TAKEN;
        else
            return BusyState.FREE;
    }
}
