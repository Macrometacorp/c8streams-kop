/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.pulsar.handlers.kop.utils;

import static org.apache.pulsar.common.naming.TopicName.PARTITIONED_TOPIC_SUFFIX;

import lombok.Getter;
import org.apache.kafka.common.TopicPartition;

/**
 * KopTopic maintains two topic name, one is the original topic name, the other is the full topic name used in Pulsar.
 * We shouldn't use the original topic name directly in KoP source code. Instead, we should
 *   1. getOriginalName() when read a Kafka request from client or write a Kafka response to client.
 *   2. getFullName() when access Pulsar resources.
 */
public class KopTopic {

    private static final String persistentDomain = "persistent://";
    private static final String topicKafka = "public/__kafka/__consumer_offsets";
    
    private static volatile String namespacePrefix;  // the full namespace prefix, e.g. "public/default"
    private static boolean isDefaultTopicTypePartitioned;

    public static String removeDefaultNamespacePrefix(String fullTopicName) {
        final String topicPrefix = persistentDomain + namespacePrefix + "/";
        if (fullTopicName.startsWith(topicPrefix)) {
            return fullTopicName.substring(topicPrefix.length());
        } else {
            return fullTopicName;
        }
    }

    public static void initialize(String namespace) {
        if (namespace.split("/").length != 2) {
            throw new IllegalArgumentException("Invalid namespace: " + namespace);
        }
        KopTopic.namespacePrefix = namespace;
    }

    public static void initialize(String namespace, boolean isDefaultTopicTypePartitioned) {
        if (namespace.split("/").length != 2) {
            throw new IllegalArgumentException("Invalid namespace: " + namespace);
        }
        KopTopic.namespacePrefix = namespace;
        KopTopic.isDefaultTopicTypePartitioned = isDefaultTopicTypePartitioned;
    }
    
    
    @Getter
    private final String originalName;
    @Getter
    private final String fullName;

    public KopTopic(String topic) {
        if (namespacePrefix == null) {
            throw new RuntimeException("KopTopic is not initialized");
        }
        originalName = topic;
        fullName = expandToFullName(topic);
    }

    private String expandToFullName(String topic) {
        if (topic.startsWith(persistentDomain)) {
        	throw new IllegalArgumentException("Invalid topic name '" + topic + 
        			"', it must not begin with 'persistent:'");
        }

        String[] parts = topic.split("/");
        if (parts.length == 3 && topic.contains(topicKafka)) {
            return persistentDomain + topic;
        } else if (parts.length == 1) {
            return persistentDomain + namespacePrefix + "/" + topic;
        } else {
            throw new IllegalArgumentException("Invalid topic name '" + topic + 
            		"', it should be simple string <topic>");
        }
    }

    public String getPartitionName(int partition) {
        if (partition < 0) {
            throw new IllegalArgumentException("Invalid partition " + partition + ", it should be non-negative number");
        }
        return isDefaultTopicTypePartitioned ? fullName + PARTITIONED_TOPIC_SUFFIX + partition: fullName;
    }

    public static String toString(TopicPartition topicPartition) {
    	KopTopic kopTopic = new KopTopic(topicPartition.topic());
        return isDefaultTopicTypePartitioned ? kopTopic.getPartitionName(topicPartition.partition()) : 
        	kopTopic.getFullName();
    }
}
