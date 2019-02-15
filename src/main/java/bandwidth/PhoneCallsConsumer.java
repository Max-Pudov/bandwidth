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

import java.util.Properties;
import kafka.consumer.ConsumerConfig;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.stream.kafka.KafkaStreamer;

public class PhoneCallsConsumer {
    public static void main(String[] args) {
        Ignite ignite = Ignition.start("client-config.xml");

        IgniteDataStreamer<String, PhoneCall> igniteStreamer = ignite.dataStreamer("PhoneCalls");

        igniteStreamer.allowOverwrite(true);
        igniteStreamer.autoFlushFrequency(1000);

        KafkaStreamer<String, PhoneCall> streamer = new KafkaStreamer<>();

        streamer.setIgnite(ignite);
        streamer.setStreamer(igniteStreamer);
        streamer.setConsumerConfig(consumerConfig());
        streamer.setTopic("ignite-PhoneCalls");
        streamer.setThreads(1);
        streamer.setSingleTupleExtractor(messageAndMetadata -> {
            String key = new String(messageAndMetadata.key());
            PhoneCall value = PhoneCall.fromBytes(messageAndMetadata.message());

            System.out.println("Consumed: " + key + " -> " + value);

            return new IgniteBiTuple<>(key, value);
        });

        streamer.start();
    }

    private static ConsumerConfig consumerConfig() {
        Properties props = new Properties();

        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", "ignite");

        return new ConsumerConfig(props);
    }
}