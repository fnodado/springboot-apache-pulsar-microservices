package net.rip.pulsar.consumer.service;


import net.rip.pulsar.consumer.entity.MyMessage;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
public class PulsarConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarConsumer.class);

    @Value("${spring.pulsar.topic.name}")
    private String topicName;

    @Value("${spring.pulsar.subscription.name}")
    private String subscriptionName;

    @Value("${pulsar.url}")
    private String pulsarUrl;

    public MyMessage receiveMessage() throws PulsarClientException {
        LOGGER.info(String.format("receiving message..."));
        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();


        Consumer<MyMessage> consumer = client.newConsumer(JSONSchema.of(MyMessage.class))
                .topic(topicName)
                .subscriptionName("subscriptionName")
                .subscribe();

        MyMessage myMessage = consumer.receive().getValue();
        myMessage.setData("message is received...");

        LOGGER.info(String.format("My message is received: %s", myMessage.toString()));

        return myMessage;
    }

}
