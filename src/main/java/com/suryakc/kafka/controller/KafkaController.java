package com.suryakc.kafka.controller;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.suryakc.kafka.consumer.DemoConsumer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

@RestController
public class KafkaController {
	
	private static final String KAFKA_PROPERTIES = "kafka.properties";
	
    private KafkaTemplate<String, String> template;
    private DemoConsumer demoConsumer;
    private Properties kafkaProperties;

    public KafkaController(KafkaTemplate<String, String> template, DemoConsumer demoConsumer) {
        this.template = template;
        this.demoConsumer = demoConsumer;
        kafkaProperties = new Properties();
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        try(InputStream resourceStream = loader.getResourceAsStream(KAFKA_PROPERTIES)) {
        	kafkaProperties.load(resourceStream);
        } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    @PostMapping("/kafka/messages")
    public void produce(@RequestParam String message) {
        template.send("newTopic", message);
    }

    @GetMapping("/kafka/messages")
    public List<String> getMessages() {
        return demoConsumer.getMessages();
    }
    
    @GetMapping("/kafka/topics")
    public Set<String> getTopics() {
    	AdminClient adminClient = AdminClient.create(kafkaProperties);
    	ListTopicsOptions listTopicsOptions = new ListTopicsOptions();
    	listTopicsOptions.listInternal(true);    	

    	try {
			return adminClient.listTopics(listTopicsOptions).names().get();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			adminClient.close();
		}
    	return null;
    }
    
    @PostMapping("/kafka/topics")
    public void createTopic(@RequestParam String topicName) {
    	AdminClient adminClient = AdminClient.create(kafkaProperties);
    	NewTopic newTopic = new NewTopic(topicName, 1, (short)1); //new NewTopic(topicName, numPartitions, replicationFactor)

    	List<NewTopic> newTopics = new ArrayList<NewTopic>();
    	newTopics.add(newTopic);

    	adminClient.createTopics(newTopics);
    	adminClient.close();
    }
}
