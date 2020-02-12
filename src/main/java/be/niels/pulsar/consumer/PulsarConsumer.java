package be.niels.pulsar.consumer;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import static be.niels.pulsar.Constants.TOPIC;
import static be.niels.pulsar.Constants.URL;
import static be.niels.pulsar.PulsarClientCreator.client;

public class PulsarConsumer {

    public static void main(String[] args) throws PulsarClientException {
        final PulsarClient client = client(URL);
        final Consumer<byte[]> consumer = createConsumerFor(TOPIC, "first-subscription", client);

        startConsuming(consumer, 2);

        consumer.close();
        client.close();
    }

    private static void startConsuming(Consumer<byte[]> consumer, int amountOfMessagesToConsume) throws PulsarClientException {
        while (amountOfMessagesToConsume > 0) {
            Message<?> receivedMessage = consumer.receive();
            try {
                System.out.printf("Message received: %s \n", new String(receivedMessage.getData()));
                // Acknowledge the message so that it can be deleted by the message broker
                consumer.acknowledge(receivedMessage);
                amountOfMessagesToConsume -= 1;
            } catch (Exception e) {
                // Message failed to process, redeliver later
                consumer.negativeAcknowledge(receivedMessage);
            }
        }
        consumer.close();
    }

    private static Consumer<byte[]> createConsumerFor(String topic, String subscriptionName, PulsarClient client) throws PulsarClientException {
        return client.newConsumer()
                .topic(topic)
                .subscriptionName(subscriptionName)
                .subscribe();
    }

}
