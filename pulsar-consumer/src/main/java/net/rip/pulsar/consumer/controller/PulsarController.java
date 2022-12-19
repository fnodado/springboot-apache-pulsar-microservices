package net.rip.pulsar.consumer.controller;

import net.rip.pulsar.consumer.service.PulsarConsumer;
import net.rip.pulsar.consumer.entity.MyMessage;

import org.apache.pulsar.client.api.PulsarClientException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequestMapping("/api/v1")
public class PulsarController {

    @Autowired
    private PulsarConsumer pulsarConsumer;

    @GetMapping("/consume")
    public ResponseEntity<MyMessage> consume() throws PulsarClientException {
        return new ResponseEntity<>(pulsarConsumer.receiveMessage(), HttpStatus.OK);
    }


}
