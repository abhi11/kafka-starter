/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.examples;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import java.util.HashMap;
import java.util.Map;

public class SimpleConsumerDemo extends Thread {
    
    private final Integer pno;
    SimpleConsumerDemo(Integer pno){
	//	t= new Thread();
	this.pno = new Integer(pno);
	System.out.println("thread for partition "+this.pno);
	//	t.start();
    }
    public void run(){
	try{
	    fetchdata();
	    try{
		Thread.sleep(1000);
	    }
	    catch(Exception e){
	    }
	}
	catch(Exception e){
	}
    }
    private /*static*/ void printMessages(ByteBufferMessageSet messageSet,Integer pno) throws UnsupportedEncodingException {
	for(MessageAndOffset messageAndOffset: messageSet) {
	    long coffset = messageAndOffset.offset();
	    ByteBuffer payload = messageAndOffset.message().payload();
	    byte[] bytes = new byte[payload.limit()];
	    payload.get(bytes);
	    System.out.println(new String(bytes, "UTF-8")+" P: "+this.pno+" and offset "+coffset);
	}
    }
  
    private /*static*/ void fetchdata() throws Exception {
	System.out.println("SimpleConsumer code....");
      
	SimpleConsumer simpleConsumer = new SimpleConsumer(KafkaProperties.kafkaServerURL,
							   KafkaProperties.kafkaServerPort,
							   KafkaProperties.connectionTimeOut,
							   KafkaProperties.kafkaProducerBufferSize,
							   KafkaProperties.clientId);

	System.out.println("Fetching partition "+this.pno);
	FetchRequest req = new FetchRequestBuilder()
            .clientId(KafkaProperties.clientId)
            .addFetch("try2", this.pno, 0L, 2000)
            .build();
	FetchResponse fetchResponse = simpleConsumer.fetch(req);
	printMessages((ByteBufferMessageSet) fetchResponse.messageSet("try2", pno),pno);

	/*
	System.out.println("Fetching partition 1");
	req = new FetchRequestBuilder()
            .clientId(KafkaProperties.clientId)
            .addFetch("try2", 1, 0L, 1000)
            .build();
	fetchResponse = simpleConsumer.fetch(req);
	printMessages((ByteBufferMessageSet) fetchResponse.messageSet("try2", 1));
	*/

    }
}
