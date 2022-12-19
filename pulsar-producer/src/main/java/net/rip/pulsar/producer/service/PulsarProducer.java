package net.rip.pulsar.producer.service;


import net.rip.pulsar.producer.entity.MyMessage;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Service
public class PulsarProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarProducer.class);

    @Value("${spring.pulsar.topic.name}")
    private String topicName;

    @Value("${pulsar.url}")
    private String pulsarUrl;

    public void sendMessage(MyMessage myMessage) throws PulsarClientException {
        LOGGER.info(String.format("My message => %s", myMessage.toString()));
        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();


        Producer<MyMessage> producer = client.newProducer(JSONSchema.of(MyMessage.class))
                .topic(topicName)
                .sendTimeout(3, TimeUnit.SECONDS)
                .create();

        producer.send(myMessage);
        LOGGER.info(String.format("My message is sent"));
    }


    public void sendMessageAsync(MyMessage myMessage) throws PulsarClientException {
        LOGGER.info(String.format("My message => %s", myMessage.toString()));
        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();


        Producer<MyMessage> producer = client.newProducer(JSONSchema.of(MyMessage.class))
                .topic(topicName)
                .sendTimeout(3, TimeUnit.SECONDS)
                .create();

        CompletableFuture<MessageId> future = producer.sendAsync(myMessage);
        future.thenAccept(msgId->{
            System.out.printf("Message with ID %s successfully sent asynchronously\n", msgId);
        });
        LOGGER.info(String.format("My message is sent"));
    }
}
