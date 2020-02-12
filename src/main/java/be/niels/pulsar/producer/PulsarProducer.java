package be.niels.pulsar.producer;

import be.niels.pulsar.Constants;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import static org.apache.pulsar.client.api.PulsarClient.builder;

public class PulsarProducer {

    public static void main(String[] args) throws PulsarClientException {

        PulsarClient client = builder()
                .serviceUrl(Constants.URL)
                .build();

        Producer<byte[]> producer = client.newProducer()
                .topic(Constants.TOPIC)
                .create();

        // You can then send messages to the broker and topic you specified:
        producer.send("My message".getBytes());


    }

}
