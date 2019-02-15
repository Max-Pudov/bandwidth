/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package bandwidth;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class PhoneCallsProducer {
    private final Producer<String, PhoneCall> kafkaProducer = createKafkaProducer();

    public static void main(String[] args) throws Exception {
        PhoneCallsProducer producer = new PhoneCallsProducer();

        List<PhoneCall> calls = new ArrayList<>(10);

        for (int i = 0; i < 10; i++) {
            PhoneCall call = new PhoneCall(i, System.currentTimeMillis());

            producer.send(call);

            calls.add(call);
        }

        System.out.println("Created 10 new calls...");

        Thread.sleep(10000);

        for (int i = 0; i < 10; i++) {
            if (i % 2 == 0) {
                PhoneCall call = calls.get(i);

                call.setEndTime(System.currentTimeMillis());

                producer.send(call);
            }
        }

        System.out.println("Completed some of the calls...");
    }

    private void send(PhoneCall call) throws Exception {
        ProducerRecord<String, PhoneCall> record = new ProducerRecord<>(
            "ignite-PhoneCalls",
            String.valueOf(call.getId()),
            call
        );

        kafkaProducer.send(record).get();
    }

    private static Producer<String, PhoneCall> createKafkaProducer() {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "PhoneCalls.producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, PhoneCallSerializer.class.getName());

        return new KafkaProducer<>(props);
    }

    public static class PhoneCallSerializer implements Serializer<PhoneCall> {
        @Override public byte[] serialize(String topic, PhoneCall call) {
            return call.toBytes();
        }

        @Override public void configure(Map<String, ?> map, boolean b) {
        }

        @Override public void close() {
        }
    }
}
