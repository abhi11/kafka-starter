/**
 * Uses the SimpleConsumerDemo class to fetch data from different partitions.
 * SimpleConsumerDemo creates a thread for each partition.
 **/

package kafka.examples;

public class MainConsumer {
    
    public static void main(String[] args) {
	SimpleConsumerDemo c1 =	new SimpleConsumerDemo(0);
	SimpleConsumerDemo c2 =	new SimpleConsumerDemo(1);
	c1.start();
	c2.start();
    }
}
