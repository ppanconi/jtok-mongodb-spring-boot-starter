package com.jtok.spring.publisher;

import com.jtok.spring.domainevent.DomainEvent;
import com.jtok.spring.domainevent.DomainEventRepository;
import com.mongodb.bulk.BulkWriteResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.List;

@Service
@Slf4j
public class MongoDbDomainEventPublisherKafka extends DomainEventPublisherKafkaSupport {

    private final MongoTemplate mongoTemplate;

    @Autowired
    public MongoDbDomainEventPublisherKafka(DomainEventRepository repository,
                                            KafkaTemplate<String, String> kafkaTemplate,
                                            MongoTemplate mongoTemplate,
                                            GenericApplicationContext context) {
        super(repository, kafkaTemplate, context);
        this.mongoTemplate = mongoTemplate;
    }

    @PostConstruct
    private void initCollections() {
	try {	
	  mongoTemplate.createCollection(DomainEvent.class);
	} catch (Exception ex) {}	
    }



    @Override
    void handlePublishingFailure(Throwable ex) {
        throw new RuntimeException("Error sending data to kafka: " + ex.getMessage(), ex);
    }

    @Override
    void handlePublishingSuccess(DomainEvent event) {
    }

    @Override
    void completePublishing(List<DomainEvent> events) {

        if (events.size() > 0) {
            log.info("updating offset to {} published domain events ... ", events.size());
            BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, DomainEvent.class);
            for (var event : events) {
                Query select = Query.query(Criteria.where("id").is(event.getId()));
                Update update = new Update();
                update.set("offSet", event.getOffSet());
                bulkOperations.updateOne(select, update);
            }

            BulkWriteResult executeResult = bulkOperations.execute();
            log.info("updated offset to {} published domain events ", executeResult.getModifiedCount());
        }
    }
}
