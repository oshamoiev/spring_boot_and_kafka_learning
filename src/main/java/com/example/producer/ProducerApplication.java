package com.example.producer;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.converter.JsonMessageConverter;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.messaging.Message;

import java.util.Map;
import java.util.Random;

import static com.example.producer.ProducerApplication.PAGE_VIEWS_TOPIC;

@SpringBootApplication
public class ProducerApplication {

    public static void main(String[] args) {
        SpringApplication.run(ProducerApplication.class, args);
    }

    public static final String PAGE_VIEWS_TOPIC = "pv_topic1";
}

@Configuration
class RunnerConfiguration {

    void kafka(KafkaTemplate<Object, Object> template) {
        var pageView = (PageView) random("kafka");
        template.send(ProducerApplication.PAGE_VIEWS_TOPIC, pageView);
    }

    private Object random(String source) {
        var names = "shamoiev,ponomar,wopopalo,mama,baba".split(",");
        var pages = "blog.html,about.html,contact.html,news.html,index.html".split(",");
        var random = new Random();
        var name = names[random.nextInt(names.length)];
        var page = pages[random.nextInt(pages.length)];
        return new PageView(page, Math.random() > .5 ? 100 : 1000, name, source);
    }

    @Bean
    ApplicationListener<ApplicationReadyEvent> runnerListener(KafkaTemplate<Object, Object> template) {
        return new ApplicationListener<ApplicationReadyEvent>() {
            @Override
            public void onApplicationEvent(ApplicationReadyEvent event) {
                kafka(template);
            }
        };
    }
}

/* Kafka configuration */
@Configuration
class KafkaConfiguration {

    @KafkaListener(topics = PAGE_VIEWS_TOPIC, groupId = "pv_topic_group")
    public void onNewPageView(Message<PageView> pageView) {
        System.out.println("-------------------");
        System.out.println("new page view " + pageView.getPayload());
        pageView.getHeaders().forEach((s, o) -> System.out.println(s + "=" + o));
    }

    @Bean
    NewTopic pageViewsTopic() {
        return new NewTopic(PAGE_VIEWS_TOPIC, 1, (short) 1);
    }

    @Bean
    JsonMessageConverter jsonMessageConverter() {
        return new JsonMessageConverter();
    }

    @Bean
    KafkaTemplate<Object, Object> kafkaTemplate(ProducerFactory<Object, Object> producerFactory) {
        return new KafkaTemplate<>(producerFactory,
                Map.of(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class));
    }
}

/* Record to transfer */
record PageView(String page, long duration, String userId, String source) {
}
