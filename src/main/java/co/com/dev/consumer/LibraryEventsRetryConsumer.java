package co.com.dev.consumer;

import co.com.dev.service.LibraryEventsService;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class LibraryEventsRetryConsumer {

    @Autowired
    private LibraryEventsService libraryEventsService;

    @KafkaListener(topics = {"${topics.retry}"},
            autoStartup = "${retry.startup:false}",
            groupId = "retry-listener-group")
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        log.info("Consumer record in Retry Consumer: {}", consumerRecord);
        consumerRecord.headers().forEach(header -> log.info("key: {}, value: {}", header.key(), header.value()));
        libraryEventsService.processLibraryEvent(consumerRecord);
    }
}
