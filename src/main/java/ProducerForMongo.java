
/**
 *   Producer for the tweets stored in Mongo
 */

package kafka.examples;

import kafka.javaapi.*;
import java.util.Properties;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class ProducerForMongo extends Thread
{
    private final kafka.javaapi.producer.Producer<Integer, String> producer;
    private final String topic;
    private final Properties props = new Properties();
    static Integer msgno=0;
    public ProducerForMongo(String topic)
    {
	props.put("serializer.class", "kafka.serializer.StringEncoder");
	props.put("metadata.broker.list", "localhost:9092");
	//props.put("partitioner.class", "example.producer.SimplePartitioner")
	// Use random partitioner. Don't need the key type. Just set it to Integer.
	// The message is of type String.
	producer = new kafka.javaapi.producer.Producer<Integer, String>(new ProducerConfig(props));
	this.topic = topic;
    }
  
    public void putdata(String messageStr) {
	msgno = msgno +1;
	producer.send(new KeyedMessage<Integer, String>(topic,messageStr));
	msgno = msgno + 1;
	System.out.println("Message No "+msgno+" Message : "+messageStr);
    }
    public void close(){
	producer.close();
    }

}
