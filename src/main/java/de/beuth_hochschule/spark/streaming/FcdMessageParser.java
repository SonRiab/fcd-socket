/*
 * Copyright (c) 2016. [j]karef GmbH
 */
package de.beuth_hochschule.spark.streaming;

import de.beuth_hochschule.fcd.ExtendedFloatingCarData;
import de.beuth_hochschule.fcd.ExtendedFloatingCarDataImpl;
import de.beuth_hochschule.fcd.taxi.BusyState;
import de.beuth_hochschule.fcd.taxi.TaxiFloatingCarDataImpl;
import de.beuth_hochschule.fcd.taxi.WaitingState;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * @author Rene Jablonski <rene@vnull.de>
 * @date 14.11.16
 */
public class FcdMessageParser {

    private static final Logger _LOG = LogManager.getLogger(FcdMessageParser.class);

    public static ExtendedFloatingCarData parse(String line) {
        ExtendedFloatingCarData extFCD = null;

        /* To ensure we also tokenize empty values, place spaces in front of the string and after each comma.
           They will be removed later. */
        line = " " + line;
        line = line.replaceAll(",",", ");

        _LOG.debug("[parse] line to parse: " + line);

        StringTokenizer tokenizer = new StringTokenizer(line, ",");
        List<String> tokens = new ArrayList<>(tokenizer.countTokens());

        /* To handle the tokens easier, we add them to a string list. We also trim the tokens here. */
        while(tokenizer.hasMoreTokens()) {
            tokens.add(tokenizer.nextToken().trim());
        }

        try {
            switch (tokens.size()) {
                case 10:
                    extFCD = parseTaxiFCD(tokens);
                    break;
                default:
                    extFCD = parseDefault(tokens);
                    break;
            }
        } catch (Exception e) {
            _LOG.error("[parse] Could not parse the following line: " + line);
            _LOG.trace(e);
        }

        _LOG.debug("[parse] parsed data: " + extFCD);

        return extFCD;
    }

    private static ExtendedFloatingCarData parseDefault(List<String> tokens) {
        String token;
        List<String> unmappedData = new ArrayList<>();

        if(tokens.size() > 5) {
            unmappedData = tokens.subList(5, tokens.size());
        }

        token = tokens.get(1).trim();
        Float speed = token.isEmpty() ? null : Float.parseFloat(token);
        token = tokens.get(3).trim();
        Double longitude = token.isEmpty() ? null : Double.parseDouble(token);
        token = tokens.get(4).trim();
        Double latitude = token.isEmpty() ? null : Double.parseDouble(token);
        return new ExtendedFloatingCarDataImpl(
                tokens.get(0),
                speed,
                tokens.get(2),
                longitude,
                latitude,
                unmappedData);
    }

    private static ExtendedFloatingCarData parseTaxiFCD(List<String> tokens) {
        String token;
        List<String> unmappedData = new ArrayList<>();

        token = tokens.get(3);
        Double longitude = token.isEmpty() ? null : Double.parseDouble(token);
        token = tokens.get(4);
        Double latitude = token.isEmpty() ? null : Double.parseDouble(token);
        token = tokens.get(5);
        WaitingState waiting = token.isEmpty() ? null : WaitingState.getState(Integer.parseInt(token));
        token = tokens.get(6);
        BusyState busy = token.isEmpty() ? null : BusyState.getState(Integer.parseInt(token));
        token = tokens.get(7);
        Double degree = token.isEmpty() ? null : Double.parseDouble(token);
        token = tokens.get(8);
        Float speed = token.isEmpty() ? null : Float.parseFloat(token);
        token = tokens.get(9);
        Integer gpsState = token.isEmpty() ? null : Integer.parseInt(token);

        return new TaxiFloatingCarDataImpl(
                tokens.get(0),
                tokens.get(1),
                tokens.get(2),
                longitude,
                latitude,
                waiting,
                busy,
                degree,
                speed,
                gpsState,
                unmappedData);
    }
}
