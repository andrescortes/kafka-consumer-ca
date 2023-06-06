package co.com.dev.consumer;

import co.com.dev.entity.Book;
import co.com.dev.entity.LibraryEvent;
import co.com.dev.entity.LibraryEventType;
import co.com.dev.jpa.LibraryEventsRepository;
import co.com.dev.service.LibraryEventsService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
@TestPropertySource(properties = {
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}",
})
class LibraryEventsConsumerIntegrationTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @SpyBean
    private LibraryEventsConsumer libraryEventsConsumerSpy;

    @SpyBean
    private LibraryEventsService libraryEventsServiceSpy;

    @Autowired
    private LibraryEventsRepository repository;

    @Autowired
    private ObjectMapper objectMapper;

    private ConsumerRecord<String, String> record;

    @BeforeEach
    void setUp() {
        for (MessageListenerContainer messageListenerContainer : kafkaListenerEndpointRegistry.getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
        }
    }

    @AfterEach
    void tearDown() {
        repository.deleteAll();
    }

    @Test
    void shouldBePublishNewLibraryEvent() throws ExecutionException, InterruptedException, JsonProcessingException {
        String json = "{\"libraryEventId\":1,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":1001,\"bookName\":\"Kafka using springboot ca\",\"bookAuthor\":\"Dilip\"}}";
        kafkaTemplate.sendDefault(json).get();

        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        Mockito.verify(libraryEventsConsumerSpy, Mockito.times(1)).onMessage(Mockito.isA(ConsumerRecord.class));
        Mockito.verify(libraryEventsServiceSpy, Mockito.times(1)).processLibraryEvent(Mockito.isA(ConsumerRecord.class));

        Iterable<LibraryEvent> libraryEvents = repository.findAll();
        Assertions.assertThat(libraryEvents).hasSize(1);
        Assertions.assertThat(libraryEvents.iterator().next().getLibraryEventId()).isNotNull();
        Assertions.assertThat(libraryEvents.iterator().next().getLibraryEventType()).isEqualTo(LibraryEventType.NEW);
        Assertions.assertThat(libraryEvents.iterator().next().getBook().getBookId()).isEqualTo(1001);
        Assertions.assertThat(libraryEvents.iterator().next().getBook().getBookName()).isEqualTo("Kafka using springboot ca");
        Assertions.assertThat(libraryEvents.iterator().next().getBook().getBookAuthor()).isEqualTo("Dilip");
    }

    @Test
    void shouldBePublishUpdateLibraryEvent() throws JsonProcessingException, ExecutionException, InterruptedException {
        String json = "{\"libraryEventId\":null,\"eventStatus\":\"ADD\",\"book\":{\"bookId\":1001,\"bookName\":\"Kafka using springboot ca\",\"bookAuthor\":\"Dilip\"}}";
        LibraryEvent libraryEvent = objectMapper.readValue(json, LibraryEvent.class);
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        repository.save(libraryEvent);

        Book book = Book.builder().bookId(123).bookName("Kafka using springboot 2.x").bookAuthor("Dilip").build();
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        libraryEvent.setBook(book);
        String jsonUpdated = objectMapper.writeValueAsString(libraryEvent);

        kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(), jsonUpdated).get();
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        Mockito.verify(libraryEventsConsumerSpy, Mockito.times(1)).onMessage(Mockito.isA(ConsumerRecord.class));
        Mockito.verify(libraryEventsServiceSpy, Mockito.times(1)).processLibraryEvent(Mockito.isA(ConsumerRecord.class));

        LibraryEvent persistedLibraryEvent = repository.findById(libraryEvent.getLibraryEventId()).get();
        Assertions.assertThat(persistedLibraryEvent.getLibraryEventType()).isEqualTo(LibraryEventType.UPDATE);
        Assertions.assertThat(persistedLibraryEvent.getBook().getBookId()).isEqualTo(123);
        Assertions.assertThat(persistedLibraryEvent.getBook().getBookName()).isEqualTo("Kafka using springboot 2.x");
        Assertions.assertThat(persistedLibraryEvent.getBook().getBookAuthor()).isEqualTo("Dilip");
    }
}
