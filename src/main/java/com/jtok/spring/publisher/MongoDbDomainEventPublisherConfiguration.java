package com.jtok.spring.publisher;

import com.jtok.spring.domainevent.DomainEventProcessor;
import com.jtok.spring.domainevent.DomainEventRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.kafka.core.KafkaTemplate;

@Configuration
@EnableMongoRepositories(basePackages = {"com.jtok.spring.domainevent"})
public class MongoDbDomainEventPublisherConfiguration {

    @Bean
    @Autowired
    public DomainEventPublisher domainEventExporter(DomainEventRepository repository,
                                                    KafkaTemplate<String, String> kafkaTemplate,
                                                    GenericApplicationContext context,
                                                    MongoTemplate mongoTemplate) {
        return new MongoDbDomainEventPublisherKafka(repository, kafkaTemplate, mongoTemplate, context);
    }

    @Bean
    @Autowired
    DomainEventProcessor domainEventProcessor(DomainEventRepository repository) {
        return new DomainEventProcessor(repository);
    }

}
