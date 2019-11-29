package com.kibria;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RuntimeContext;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
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

		/**
		 *
		 */
		private static final long serialVersionUID = -3744524525689156834L;

		public MyExtractor() {
			super(Time.seconds(10));
		}

		@Override
		public long extractTimestamp(TaxiRideEvent event) {
			return event.getTpep_dropoff_datetime().getTime();
		}
	}

	private static class ReduceBySummingTip implements ReduceFunction<TaxiRideEvent> {
		/**
		 *
		 */
		private static final long serialVersionUID = 8381252998852833752L;

		public TaxiRideEvent reduce(TaxiRideEvent r1, TaxiRideEvent r2) {
			//Combining the tip and setting it to r1 and then returning r1
			r1.setTip_amount(r1.getTip_amount() + r2.getTip_amount());
		  	return r1;
		}
	}
	  
	  private static class TotalTipForThisWindow extends ProcessWindowFunction<
		TaxiRideEvent, Tuple3<Integer, Double, Long>, Integer, TimeWindow> {
	  
		/**
		 *
		 */
		private static final long serialVersionUID = 3969223284652012611L;

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

		
		/**
		 *
		 */
		private static final long serialVersionUID = -8675408720987118260L;

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
			jedis.close();
		}
	}


	public static class RedisExampleMapper implements RedisMapper<Tuple2<String, String>>{

		/**
		 *
		 */
		private static final long serialVersionUID = 5577018346101627548L;

		@Override
		public RedisCommandDescription getCommandDescription() {
			return new RedisCommandDescription(RedisCommand.SET);
		}
	
		@Override
		public String getKeyFromData(Tuple2<String, String> data) {
			return data.f0;
		}
	
		@Override
		public String getValueFromData(Tuple2<String, String> data) {
			return data.f1;
		}
	}

	//========================Other Required Classes End=================================

	public static void main(String[] args) throws Exception {

	

		// create execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		// start a checkpoint every 1000 ms
		env.enableCheckpointing(1000);
		// advanced options:

		// set mode to exactly-once (this is the default)
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

		// make sure 500 ms of progress happen between checkpoints
		env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

		// checkpoints have to complete within one minute, or are discarded
		env.getCheckpointConfig().setCheckpointTimeout(60000);

		// allow only one checkpoint to be in progress at the same time
		env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

		// enable externalized checkpoints which are retained after job cancellation
		env.getCheckpointConfig().enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

		// allow job recovery fallback to checkpoint when there is a more recent savepoint
		env.getCheckpointConfig().setPreferCheckpointForRecovery(true);


		//Setting to work with EventTime
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		
		Properties properties = new Properties();
		
		properties.setProperty("bootstrap.servers", "localhost:9092");
		// only required for Kafka 0.8
		properties.setProperty("zookeeper.connect", "localhost:2181");
		properties.setProperty("group.id", "test");
		
		

		// parse user parameters
		//ParameterTool parameterTool = ParameterTool.fromArgs(args);

		DataStream<TaxiRideEvent> messageStream = env.addSource(new FlinkKafkaConsumer011<>("customerstreamapp-input", new TaxiRideSerializer(), properties));


		//Assigning timestamp to each event 
		DataStream<TaxiRideEvent> msgStreamWithTSandWM = messageStream.assignTimestampsAndWatermarks(new MyExtractor());

		DataStream<Tuple3<Integer,Double,Long>> tipByDestination = msgStreamWithTSandWM.
																keyBy(r -> r.getPULocationID()).
																window(SlidingEventTimeWindows.of(Time.hours(1),Time.minutes(30))).
																trigger(CountTrigger.of(1)).
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
			new ElasticsearchSink<>(config, transportAddresses, new ESTotalTipInserter()))
			.name("Elasticsearch(Batch)");
		

		//===================================================
		
		//========================disseminating result back to customer=================


		/*
		FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<String>(
			"localhost:9092",            // broker list
			"customer_realtime_topic",                  // target topic
			new SimpleStringSchema()); 

		*/

		FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build();


		//Mapping T3 to T2
		tipByDestination.map(new MapFunction<Tuple3<Integer,Double,Long>,Tuple2<String,String>>() {

			/**
			 *
			 */
			private static final long serialVersionUID = -7375869054845432499L;

			@Override
			public Tuple2<String,String> map(Tuple3<Integer, Double, Long> record) throws Exception {				
				
				ObjectMapper mapperObj = new ObjectMapper();

				// construct JSON document to index
				Map<String, String> json = new HashMap<>();				
				json.put("location_id", record.f0.toString());  // location id			
				json.put("total_tip", record.f1.toString());      // isStart

				
				String jsonResp = mapperObj.writeValueAsString(json);
				
				return new Tuple2<>("L"+record.f0.toString(),jsonResp);
				

			}
			
		}).addSink(new RedisSink<Tuple2<String, String>>(conf, new RedisExampleMapper())).name("Realtime View(Redis)");
		

		//maxTipDest.print();
		env.execute("Tips per location");



	
	}
}