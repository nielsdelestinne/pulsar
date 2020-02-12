package be.niels.pulsar;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import static org.apache.pulsar.client.api.PulsarClient.builder;

public class PulsarClientCreator {

    public static PulsarClient client(String pulsarConnectionUrl) throws PulsarClientException {
        return builder()
                .serviceUrl(pulsarConnectionUrl)
                .build();
    }

}
