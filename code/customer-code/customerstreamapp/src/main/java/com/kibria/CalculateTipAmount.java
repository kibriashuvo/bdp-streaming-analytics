package com.kibria;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


/**
 * The data type stored in the state
 */
class TotalTipWithTimestamp {

    public Integer key;
    public Double total;
    public long lastModified;
}

//Template: ProcessWindowFunction<IN, OUT, KEY, W extends Window>

public class CalculateTipAmount extends ProcessWindowFunction<TaxiRideEvent,Tuple2<Integer,String>,Integer,TimeWindow>{
    

    /**
     * Evaluates the window and outputs none or several elements.
     *
     * @param key The key for which this window is evaluated.
     * @param context The context in which the window is being evaluated.
     * @param elements The elements in the window being evaluated.
     * @param out A collector for emitting elements.
     *
     * @throws Exception The function may throw exceptions to fail the program and trigger recovery.
     */
    @Override
    public void process(Integer key,
            Context ctx,
            Iterable<TaxiRideEvent> elements, Collector<Tuple2<Integer,String>> out) throws Exception {
        Double totalTips = 0.0;
        for (TaxiRideEvent tr:elements){
            totalTips += tr.getTip_amount();
        }
        out.collect(new Tuple2<>(key,"Total tip for this location = "+totalTips));
    }

}
