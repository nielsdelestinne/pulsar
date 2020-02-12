package be.niels.pulsar.consumer;

import be.niels.pulsar.Constants;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import static org.apache.pulsar.client.api.PulsarClient.builder;

public class PulsarConsumer {

    public static void main(String[] args) throws PulsarClientException {
        PulsarClient client = builder()
                .serviceUrl(Constants.URL)
                .build();

        Consumer consumer = client.newConsumer()
                .topic(Constants.TOPIC)
                .subscriptionName("my-subscription")
                .subscribe();

        while (true) {
            // Wait for a message
            Message<?> msg = consumer.receive();

            try {
                // Do something with the message
                System.out.printf("Message received: %s", new String(msg.getData()));

                // Acknowledge the message so that it can be deleted by the message broker
                consumer.acknowledge(msg);
            } catch (Exception e) {
                // Message failed to process, redeliver later
                consumer.negativeAcknowledge(msg);
            }
        }
    }

}
