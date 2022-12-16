package net.rip.pulsar.consumer.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MyMessage {

    private long id;
    private String data;
    private long transactionId;

}
