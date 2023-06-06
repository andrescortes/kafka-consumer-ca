package co.com.dev.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

//@Component
@Slf4j
public class LibraryEventsConsumerManualOffset implements AcknowledgingMessageListener<Integer, String> {

    /**
     * Invoked with data from kafka.
     *
     * @param consumerRecord           the data to be processed.
     * @param acknowledgment the acknowledgment.
     */
    @Override
    @KafkaListener(topics = {"library-events"})
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord, Acknowledgment acknowledgment) {
        log.info("Received data: {}", consumerRecord);
        log.info("Acknowledgment is: {}", acknowledgment);
        acknowledgment.acknowledge();
    }
}
