/**
   Consumes data.
 **/
package kafka.examples;

public class KafkaConsumer implements KafkaProperties
{
  public static void main(String[] args)
  {
      Consumer consumerThread = new Consumer("try2");
      consumerThread.readdata();
  }
}
