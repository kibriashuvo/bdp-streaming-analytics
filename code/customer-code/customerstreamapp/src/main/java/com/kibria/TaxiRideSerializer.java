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

import redis.clients.jedis.Jedis;

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
        Jedis j = new Jedis("localhost", 6379);
        for(int i=1; i<264; i++ )
            System.out.println(j.get(String.valueOf(i)));
        
    }
}