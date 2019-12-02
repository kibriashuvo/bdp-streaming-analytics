package com.kibria;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;



import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;



public class TaxiRideSerializer extends AbstractDeserializationSchema<TaxiRideEvent> {

    /**
     *
     */
    private static final long serialVersionUID = -2692992753403782353L;

    public TaxiRideEvent deserialize(byte[] message) throws IOException {
        TaxiRideEvent tRideEvent = null;
        ObjectMapper objectMapper = new ObjectMapper();
        
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        objectMapper.setDateFormat(df);
        objectMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
        try{
            tRideEvent = objectMapper.readValue(message, TaxiRideEvent.class);
        }catch(Exception e){   
            TaxiRideEvent errorRide = new TaxiRideEvent();   
            errorRide.setStore_and_fwd_flag("jsonParseError "+new String(message));    
            tRideEvent = errorRide; 
        }
        
        
        return tRideEvent;
    }

}