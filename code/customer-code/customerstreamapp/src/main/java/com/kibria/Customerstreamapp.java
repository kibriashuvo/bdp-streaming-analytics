package com.kibria;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.fasterxml.jackson.databind.ObjectMapper;

import redis.clients.jedis.Jedis;


public class Customerstreamapp {

	 
	//========================Other Required Classes=================================

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

	private static class ReduceBySummingTip implements ReduceFunction<TaxiRideEvent> {
		public TaxiRideEvent reduce(TaxiRideEvent r1, TaxiRideEvent r2) {
			//Combining the tip and setting it to r1 and then returning r1
			r1.setTip_amount(r1.getTip_amount() + r2.getTip_amount());
		  	return r1;
		}
	}
	  
	  private static class TotalTipForThisWindow extends ProcessWindowFunction<
		TaxiRideEvent, Tuple3<Integer, Double, Long>, Integer, TimeWindow> {
	  
		@Override
		public void process(
		  Integer key,
		  Context context,
		  Iterable<TaxiRideEvent> summedTrip,
		  Collector<Tuple3<Integer, Double, Long>> out) {
			  	  
		  TaxiRideEvent sum = summedTrip.iterator().next();
		  //Key,SUM,end of window
		  out.collect(new Tuple3<>(key,sum.getTip_amount(),context.window().getEnd()));
		}
	}


	public static class ESTotalTipInserter
		implements ElasticsearchSinkFunction<Tuple3<Integer, Double, Long>> {
		
		

		
		// construct index request
		@Override
		public void process(
			Tuple3<Integer, Double, Long> record,
			RuntimeContext ctx,
			RequestIndexer indexer) {

			Jedis jedis = new Jedis("localhost", 6379);

			// construct JSON document to index
			Map<String, String> json = new HashMap<>();
			json.put("time", record.f2.toString());         // timestamp
			json.put("location_id", record.f0.toString());  // locatin id
			json.put("location", jedis.get(record.f0.toString()));  // location co-ordinate
			json.put("total_tip", record.f1.toString());      // isStart
		         

			IndexRequest rqst = Requests.indexRequest()
				.index("nyc-idx")           // index name
				.type("popular-locations")     // mapping name
				.source(json);

			indexer.add(rqst);
		}
	}

	//========================Other Required Classes End=================================

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

		DataStream<TaxiRideEvent> messageStream = env.addSource(new FlinkKafkaConsumer011<>("mytopic", new TaxiRideSerializer(), properties));


		//Assigning timestamp to each event 
		DataStream<TaxiRideEvent> msgStreamWithTSandWM = messageStream.assignTimestampsAndWatermarks(new MyExtractor());

		DataStream<Tuple3<Integer,Double,Long>> tipByDestination = msgStreamWithTSandWM.
																keyBy(r -> r.getPULocationID()).
																window(SlidingEventTimeWindows.of(Time.hours(1),Time.minutes(30))).
																//allowedLateness(Time.seconds(10)).
																reduce(new ReduceBySummingTip(),new TotalTipForThisWindow());

		//DataStream<Tuple2<Integer,String>> maxTipDest = tipByDestination.
		//												timeWindowAll(Time.hours(1)).maxBy(2);
		


		//==============Adding sink============================

		//For elasticsearch

		Map<String, String> config = new HashMap<>();
		config.put("bulk.flush.max.actions", "1");   // flush inserts after every event
		config.put("cluster.name", "elasticsearch"); // default cluster name

	
		List<InetSocketAddress> transportAddresses = new ArrayList<>();
		transportAddresses.add(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300));
		
	

		tipByDestination.addSink(
			new ElasticsearchSink<>(config, transportAddresses, new ESTotalTipInserter()));
		

		//===================================================
		
		//========================disseminating result back to customer=================


		FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<String>(
			"localhost:9092",            // broker list
			"customer_realtime_topic",                  // target topic
			new SimpleStringSchema()); 
		tipByDestination.map(new MapFunction<Tuple3<Integer,Double,Long>,String>() {

			@Override
			public String map(Tuple3<Integer, Double, Long> record) throws Exception {				
				//Jedis jedis = new Jedis("localhost", 6379);
				ObjectMapper mapperObj = new ObjectMapper();

				// construct JSON document to index
				Map<String, String> json = new HashMap<>();
				//json.put("time", record.f2.toString());         // timestamp
				json.put("location_id", record.f0.toString());  // locatin id
				//json.put("location", jedis.get(record.f0.toString()));  // location co-ordinate
				json.put("total_tip", record.f1.toString());      // isStart

				
				String jsonResp = mapperObj.writeValueAsString(json);
				
				return jsonResp;
				

			}
			
		}).addSink(myProducer);


		
		

		

		
		//maxTipDest.print();
		env.execute("Tips per location");



	
	}
}