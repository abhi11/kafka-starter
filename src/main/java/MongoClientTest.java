/*
  Retrieves data from mongo. All the tweets in json format.
  To be used by the kafka producer and conusmed by a storm-topology.
 */

package kafka.examples;

import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;
import com.mongodb.ServerAddress;

//import java.util.Arrays;

class MongoClientTest {
    
    //ProducerForMongo pro;
    
    public static void main(String[] args){
	ProducerForMongo pro = new ProducerForMongo("Twitter");
	try
	    {
		Mongo mc = new Mongo("localhost",27017);
		DB db = mc.getDB("test_database");
		DBCollection coll = db.getCollection("try1");
		DBCursor cur = coll.find();
		try{
		    while(cur.hasNext()){
			DBObject obj = cur.next();
			String	messageStr = obj.toString();
			pro.run(messageStr);
			System.out.println(messageStr);
		    }
		}
		finally{
		    cur.close();
		}
		mc.close();
	    }

	catch(Exception e){
	    System.out.println("UnknownHostException: "+e);
	}
    }
}
