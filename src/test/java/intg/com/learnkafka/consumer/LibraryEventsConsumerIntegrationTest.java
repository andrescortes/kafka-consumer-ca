package com.learnkafka.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.entity.Book;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.entity.LibraryEventType;
import com.learnkafka.jpa.FailureRecordRepository;
import com.learnkafka.jpa.LibraryEventsRepository;
import com.learnkafka.service.LibraryEventsService;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest
@EmbeddedKafka(topics = {"library-events", "library-events.RETRY",
    "library-events.DLT"}, partitions = 3)
@TestPropertySource(properties = {
    "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
    "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}",
    "retryListener.startup=false"
})
class LibraryEventsConsumerIntegrationTest {

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    KafkaListenerEndpointRegistry endpointRegistry;

    @SpyBean
    LibraryEventsConsumer libraryEventsConsumerSpy;

    @SpyBean
    LibraryEventsService libraryEventsServiceSpy;

    @Autowired
    LibraryEventsRepository libraryEventsRepository;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    private FailureRecordRepository failureRecordRepository;

    private Consumer<Integer, String> consumer;

    @Value("${topics.retry}")
    private String retryTopic;

    @Value("${topics.dlt}")
    private String deadLetterTopic;

    @BeforeEach
    void setUp() {

        var container = endpointRegistry.getListenerContainers()
            .stream().filter(messageListenerContainer ->
                Objects.equals(messageListenerContainer.getGroupId(),
                    "library-events-listener-group"))
            .collect(Collectors.toList()).get(0);
        ContainerTestUtils.waitForAssignment(container,
            embeddedKafkaBroker.getPartitionsPerTopic());
//        for (MessageListenerContainer messageListenerContainer : endpointRegistry.getListenerContainers()){
//            ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
//        }

    }

    @AfterEach
    void tearDown() {
        libraryEventsRepository.deleteAll();
    }

    @Test
    @Timeout(value = 5)
    void publishNewLibraryEvent()
        throws ExecutionException, InterruptedException, JsonProcessingException {
        //given
        String json = " {\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        kafkaTemplate.sendDefault(json).get();

        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        //then
        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        List<LibraryEvent> libraryEventList = (List<LibraryEvent>) libraryEventsRepository.findAll();
        assert libraryEventList.size() == 1;
        libraryEventList.forEach(libraryEvent -> {
            assert libraryEvent.getLibraryEventId() != null;
            assertEquals(456, libraryEvent.getBook().getBookId());
        });

    }

    @Test
    @Timeout(value = 5)
    void publishUpdateLibraryEvent()
        throws JsonProcessingException, ExecutionException, InterruptedException {
        //given
        String json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        LibraryEvent libraryEvent = objectMapper.readValue(json, LibraryEvent.class);
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);
        //publish the update LibraryEvent

        Book updatedBook = Book.builder().
            bookId(456).bookName("Kafka Using Spring Boot 2.x").bookAuthor("Dilip").build();
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        libraryEvent.setBook(updatedBook);
        String updatedJson = objectMapper.writeValueAsString(libraryEvent);
        kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(), updatedJson).get();

        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        //then
        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));
        LibraryEvent persistedLibraryEvent = libraryEventsRepository.findById(
            libraryEvent.getLibraryEventId()).get();
        assertEquals("Kafka Using Spring Boot 2.x", persistedLibraryEvent.getBook().getBookName());
    }

    @Test
    @Timeout(value = 5)
    void publishModifyLibraryEvent_Not_A_Valid_LibraryEventId()
        throws JsonProcessingException, ExecutionException, InterruptedException {
        //given

        String json = "{\"libraryEventId\":123,\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        kafkaTemplate.sendDefault(json).get();

        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);

        //then
        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));
    }

    @Test
    @Timeout(value = 5)
    void publishUpdateLibraryEvent_null_LibraryEvent()
        throws JsonProcessingException, ExecutionException, InterruptedException {
        //given

        String json = "{\"libraryEventId\":null,\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        kafkaTemplate.sendDefault(json).get();

        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);

        //then
        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        Map<String, Object> configs = new HashMap<>(
            KafkaTestUtils.consumerProps("group2", "true", embeddedKafkaBroker));
        consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(),
            new StringDeserializer()).createConsumer();
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, deadLetterTopic);

        ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer,
            deadLetterTopic);
        System.out.println("consumerRecord is : " + consumerRecord.value());
        assertEquals(json, consumerRecord.value());
    }

    @Test
    @Timeout(value = 5)
    void publishUpdateLibraryEvent_null_LibraryEvent_failurerecord()
        throws JsonProcessingException, ExecutionException, InterruptedException {
        //given

        String json = "{\"libraryEventId\":null,\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        kafkaTemplate.sendDefault(json).get();

        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);

        //then
        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        var count = failureRecordRepository.count();
        assertEquals(1, count);

        failureRecordRepository.findAll()
            .forEach(failureRecord -> {
                System.out.println("failureRecord : " + failureRecord);
            });

    }

    @Test
    @Timeout(value = 7)
    void publishUpdateLibraryEvent_999_LibraryEvent()
        throws JsonProcessingException, ExecutionException, InterruptedException {
        //given

        String json = "{\"libraryEventId\":999,\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        kafkaTemplate.sendDefault(json).get();

        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);

        //then
        verify(libraryEventsConsumerSpy, times(3)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(3)).processLibraryEvent(isA(ConsumerRecord.class));

        Map<String, Object> configs = new HashMap<>(
            KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker));
        consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(),
            new StringDeserializer()).createConsumer();
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, retryTopic);

        ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer,
            retryTopic);
        System.out.println("consumerRecord is : " + consumerRecord.value());
        assertEquals(json, consumerRecord.value());

    }


}
