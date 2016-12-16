/*
 * Copyright (c) 2016. [j]karef GmbH
 */
package de.beuth_hochschule.spark.streaming;

import de.beuth_hochschule.fcd.ExtendedFloatingCarData;
import de.beuth_hochschule.fcd.ExtendedFloatingCarDataImpl;
import de.beuth_hochschule.fcd.taxi.TaxiFloatingCarData;
import de.beuth_hochschule.fcd.taxi.TaxiFloatingCarDataImpl;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * @author Rene Jablonski <rene@vnull.de>
 * @date 16.12.16
 */
public class FcdMessageParserTest {

    private ExtendedFloatingCarData data;
    private String line;

    private void checkExtFCDWithValues(ExtendedFloatingCarData data) {
        assertNotNull(data);
        assertTrue(data instanceof ExtendedFloatingCarDataImpl);
        assertFalse(data.isDirty());
        assertFalse(data.getId().isEmpty());
        assertFalse(data.getTimestamp().isEmpty());
        assertNotNull(data.getLongitude());
        assertNotNull(data.getLatitude());
        assertNotNull(data.getSpeed());
    }

    private void checkExtFCDWithEmptyValues(ExtendedFloatingCarData data) {
        assertNotNull(data);
        assertTrue(data instanceof ExtendedFloatingCarDataImpl);
        assertTrue(data.isDirty());
        assertTrue(data.getId().isEmpty());
        assertTrue(data.getTimestamp().isEmpty());
        assertNull(data.getLongitude());
        assertNull(data.getLatitude());
        assertNull(data.getSpeed());
    }

    @Test
    public void parseTenEmptyValues() throws Exception {
        line = ",,,,,,,,,";
        data = FcdMessageParser.parse(line);
        checkExtFCDWithEmptyValues(data);
        assertTrue(data.getUnmappedData().size() == 0);
        assertFalse(data.getExtendedData().isEmpty());
        assertTrue(data instanceof TaxiFloatingCarDataImpl);
        TaxiFloatingCarData taxiData = (TaxiFloatingCarData) data;
        assertTrue(taxiData.getTaxiId().isEmpty());
        assertNull(taxiData.getWaitingState());
        assertNull(taxiData.getBusyState());
        assertNull(taxiData.getDegree());
        assertNull(taxiData.getGpsState());
    }

    @Test
    public void parseTenValues() throws Exception {
        line = "012345,98765,2016-12-24 18:00,54.534,8.735,0,0,45.9,55.5,0";
        data = FcdMessageParser.parse(line);
        checkExtFCDWithValues(data);
        assertTrue(data.getUnmappedData().size() == 0);
        assertFalse(data.getExtendedData().isEmpty());
        assertTrue(data instanceof TaxiFloatingCarDataImpl);
        TaxiFloatingCarData taxiData = (TaxiFloatingCarData) data;
        assertFalse(taxiData.getTaxiId().isEmpty());
        assertNotNull(taxiData.getWaitingState());
        assertNotNull(taxiData.getBusyState());
        assertNotNull(taxiData.getDegree());
        assertNotNull(taxiData.getGpsState());
    }

    @Test
    public void parseTenValuesInWrongOrder() throws Exception {
        line = "012345,2016-12-24 18:00,98765,54.534,8.735,45.9,55.5,0,0,0";
        data = FcdMessageParser.parse(line);
        assertNull(data);
    }

    @Test
    public void parseTenValuesWithEmptyOne() throws Exception {
        line = "012345,,2016-12-24 18:00,54.534,8.735,0,,45.9,55.5,0";
        data = FcdMessageParser.parse(line);
        assertNotNull(data);
        assertTrue(data instanceof TaxiFloatingCarDataImpl);
        assertTrue(data.isDirty());
        TaxiFloatingCarData taxiData = (TaxiFloatingCarData) data;
        assertTrue(taxiData.getTaxiId().isEmpty());
        assertNotNull(taxiData.getWaitingState());
        assertNull(taxiData.getBusyState());
        assertNotNull(taxiData.getDegree());
        assertNotNull(taxiData.getGpsState());
    }

    @Test
    public void parseFiveEmptyValues() throws Exception {
        line = ",,,,";
        data = FcdMessageParser.parse(line);
        checkExtFCDWithEmptyValues(data);
        assertTrue(data.getUnmappedData().size() == 0);
        assertFalse(data.getExtendedData().isEmpty());
    }

    @Test
    public void parseFiveValues() throws Exception {
        line = "012345,55.5,2016-12-24 18:00,54.534,8.735";
        data = FcdMessageParser.parse(line);
        checkExtFCDWithValues(data);
        assertTrue(data.getUnmappedData().size() == 0);
        assertFalse(data.getExtendedData().isEmpty());
    }

    @Test
    public void parseFiveValuesWithEmptyOne() throws Exception {
        line = ",55.5,,,";
        data = FcdMessageParser.parse(line);
        assertNotNull(data);
        assertTrue(data.isDirty());
        assertTrue(data.getId().isEmpty());
        assertNotNull(data.getSpeed());
        assertTrue(data.getTimestamp().isEmpty());
        assertNull(data.getLongitude());
        assertNull(data.getLatitude());
    }

    @Test
    public void parseSixEmptyValues() throws Exception {
        line = ",,,,,";
        data = FcdMessageParser.parse(line);
        checkExtFCDWithEmptyValues(data);
        List<String> unmappedData = data.getUnmappedData();
        assertTrue(unmappedData.size() == 1);
        assertTrue(unmappedData.get(0).isEmpty());
        assertFalse(data.getExtendedData().isEmpty());
        assertFalse(data instanceof TaxiFloatingCarDataImpl);
    }

    @Test
    public void parseSixValues() throws Exception {
        line = "012345,55.5,2016-12-24 18:00,54.534,8.735,Test";
        data = FcdMessageParser.parse(line);
        checkExtFCDWithValues(data);
        List<String> unmappedData = data.getUnmappedData();
        assertTrue(unmappedData.size() == 1);
        assertEquals("Test", unmappedData.get(0));
        assertFalse(data.getExtendedData().isEmpty());
        assertFalse(data instanceof TaxiFloatingCarDataImpl);
    }

}
