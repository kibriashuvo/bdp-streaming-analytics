package com.kibria;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import java.util.Properties;


public class Customerstreamapp {

	public static class MyExtractor
		extends BoundedOutOfOrdernessTimestampExtractor<TaxiRideEvent> {

		public MyExtractor() {
			super(Time.seconds(10));
		}

		@Override
		public long extractTimestamp(TaxiRideEvent event) {
			return event.getTpep_dropoff_datetime().getTime();
		}
	}

  public static void main(String[] args) throws Exception {

    // create execution environment
	StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
	

	//Setting to work with EventTime
	env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    
    Properties properties = new Properties();
    
    properties.setProperty("bootstrap.servers", "localhost:9092");
    // only required for Kafka 0.8
    properties.setProperty("zookeeper.connect", "localhost:2181");
	properties.setProperty("group.id", "test");
	
    

	// parse user parameters
	//ParameterTool parameterTool = ParameterTool.fromArgs(args);

	DataStream<TaxiRideEvent> messageStream = env.addSource(new FlinkKafkaConsumer<>("mytopic", new TaxiRideSerializer(), properties));


	//Assigning timestamp to each event 
	DataStream<TaxiRideEvent> msgStreamWithTSandWM = messageStream.assignTimestampsAndWatermarks(new MyExtractor());

	DataStream<Tuple2<Integer,String>> tipByDestination = msgStreamWithTSandWM.
															keyBy(r -> r.getDOLocationID()).
															window(SlidingEventTimeWindows.of(Time.minutes(5),Time.minutes(1))).
															process(new CalculateTipAmount());

	//DataStream<Tuple2<Integer,String>> maxTipDest = tipByDestination.
	//												timeWindowAll(Time.hours(1)).maxBy(2);
	

	// print() will write the contents of the stream to the TaskManager's standard out stream
	// the rebelance call is causing a repartitioning of the data so that all machines
	// see the messages (for example in cases when "num kafka partitions" < "num flink operators"
	/*
	messageStream.rebalance().map(new MapFunction<TaxiRideEvent, String>() {
		private static final long serialVersionUID = -6867736771747690202L;

		@Override
		public String map(TaxiRideEvent event) throws Exception {
			return "Kafka and Flink says: " + event.getStore_and_fwd_flag();
		}
	}).print();
	*/
	tipByDestination.print();
	
	//maxTipDest.print();
	env.execute();


   
  }
}