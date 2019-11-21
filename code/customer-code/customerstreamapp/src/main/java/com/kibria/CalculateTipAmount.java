package com.kibria;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


/**
 * The data type stored in the state
 */
class TotalTipWithTimestamp {

    public Integer key;
    public Double total;
    public long lastModified;
}



public class CalculateTipAmount extends KeyedProcessFunction<Integer,TaxiRideEvent,Tuple2<Integer,String>>{
    private ValueState<TotalTipWithTimestamp> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", TotalTipWithTimestamp.class));
    }

    @Override
    public void processElement(TaxiRideEvent rideEvent,
            Context ctx,
            Collector<Tuple2<Integer, String>> out) throws Exception {

        //Retriving the current total per key
        TotalTipWithTimestamp currTot = state.value();
        //If null then initializing
        if(currTot == null ){
            currTot = new TotalTipWithTimestamp();
            currTot.key = rideEvent.getDOLocationID();
            currTot.total = 0.0;
        }
        //Update the state's count

        currTot.total += rideEvent.getTip_amount();

        currTot.lastModified = ctx.timestamp();

        state.update(currTot);

        ctx.timerService().registerEventTimeTimer(currTot.lastModified+60000);

        
    }

    @Override
    public void onTimer(long timestamp,
            OnTimerContext ctx,
            Collector<Tuple2<Integer, String>> out) throws Exception {
        

        // get the state for the key that scheduled the timer
        TotalTipWithTimestamp result = state.value();

        // check if this is an outdated timer or the latest timer
        if (timestamp == result.lastModified + 60000) {
            // emit the state on timeout
            out.collect(new Tuple2<Integer, String>(result.key, "Total tip for this location= "+result.total));
        }
                
    }

}
