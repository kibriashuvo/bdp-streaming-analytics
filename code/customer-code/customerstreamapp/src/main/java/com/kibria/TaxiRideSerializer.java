package com.kibria;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;


import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

public class TaxiRideSerializer extends AbstractDeserializationSchema<TaxiRideEvent> {

    public TaxiRideEvent deserialize(byte[] message) throws IOException {
        TaxiRideEvent tRideEvent = null;
        ObjectMapper objectMapper = new ObjectMapper();
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        objectMapper.setDateFormat(df);
        objectMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
        tRideEvent = objectMapper.readValue(message, TaxiRideEvent.class);
        
        return tRideEvent;
    }

    public static void main(String[] args) {
        byte[] passThis = "{\"VendorID\":1,\"tpep_pickup_datetime\":\"2019-01-01 00:09:40\",\"tpep_dropoff_datetime\":\"2019-01-01 00:15:00\",\"passenger_count\":3,\"trip_distance\":0.6,\"RatecodeID\":1,\"store_and_fwd_flag\":\"N\",\"PULocationID\":137,\"DOLocationID\":233,\"payment_type\":1,\"fare_amount\":5.5,\"extra\":0.5,\"mta_tax\":0.5,\"tip_amount\":1.35,\"tolls_amount\":0.0,\"improvement_surcharge\":0.3,\"total_amount\":8.15,\"congestion_surcharge\":null}".getBytes();
        TaxiRideSerializer tRideSerializer = new TaxiRideSerializer();
        try {
            TaxiRideEvent tRideEvent = tRideSerializer.deserialize(passThis);
            System.out.println(tRideEvent.getStore_and_fwd_flag());
            
        } catch (Exception e) {            
            e.printStackTrace();
        }
        
    }
}