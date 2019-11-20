package com.kibria;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import java.util.Properties;


public class Customerstreamapp {

  public static void main(String[] args) throws Exception {

    // create execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    
    Properties properties = new Properties();
    
    properties.setProperty("bootstrap.servers", "localhost:9092");
    // only required for Kafka 0.8
    properties.setProperty("zookeeper.connect", "localhost:2181");
	properties.setProperty("group.id", "test");
	
    

	// parse user parameters
	//ParameterTool parameterTool = ParameterTool.fromArgs(args);

	DataStream<String> messageStream = env.addSource(new FlinkKafkaConsumer<>("mytopic", new SimpleStringSchema(), properties));

	// print() will write the contents of the stream to the TaskManager's standard out stream
	// the rebelance call is causing a repartitioning of the data so that all machines
	// see the messages (for example in cases when "num kafka partitions" < "num flink operators"
	messageStream.rebalance().map(new MapFunction<String, String>() {
		private static final long serialVersionUID = -6867736771747690202L;

		@Override
		public String map(String value) throws Exception {
			return "Kafka and Flink says: " + value;
		}
	}).print();

	env.execute();


   
  }
}