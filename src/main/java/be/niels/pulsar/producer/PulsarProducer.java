package be.niels.pulsar.producer;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import static be.niels.pulsar.Constants.TOPIC;
import static be.niels.pulsar.Constants.URL;
import static be.niels.pulsar.PulsarClientCreator.client;

public class PulsarProducer {

    public static void main(String[] args) throws PulsarClientException {

        final PulsarClient client = client(URL);
        final Producer<byte[]> producer = createProducerFor(TOPIC, client);

        producer.send("First Message".getBytes());
        producer.send("Second Message".getBytes());
        producer.send("Third Message".getBytes());

        producer.close();
        client.close();
    }

    private static Producer<byte[]> createProducerFor(String topic, PulsarClient client) throws PulsarClientException {
        return client.newProducer()
                .topic(topic)
                .create();
    }

}
