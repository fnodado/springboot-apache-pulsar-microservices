package net.rip.pulsar.producer.controller;

import net.rip.pulsar.producer.entity.MyMessage;
import net.rip.pulsar.producer.service.PulsarProducer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1")
public class PulsarController {

    @Autowired
    private PulsarProducer pulsarProducer;


    @PostMapping("/send")
    public String sendMessage(@RequestBody MyMessage myMessage){
        try {
            pulsarProducer.sendMessage(myMessage);
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }

        return "Message is sent";
    }
    @PostMapping("/send/async")
    public String sendMessageAsync(@RequestBody MyMessage myMessage){
        try {
            pulsarProducer.sendMessageAsync(myMessage);
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }

        return "Message is sent";
    }

}
