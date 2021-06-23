package de.faweizz.sharing

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.time.Duration

fun main() {
    val clientName = System.getProperty("client-name")
    val clientSecret = System.getProperty("client-secret")
    val consumerGroup = System.getProperty("consumer-group")
    val inputTopic = System.getProperty("input-topic")
    val outputTopic = System.getProperty("output-topic")
    val trustStoreLocation = System.getProperty("truststore-location")
    val trustStorePassword = System.getProperty("truststore-password")

    val config = Config(
        clientName = clientName,
        clientSecret = clientSecret,
        consumerGroup = consumerGroup,
        trustStoreLocation = trustStoreLocation,
        trustStorePassword = trustStorePassword
    )

    val consumer = KafkaConsumer<String, String>(config)
    consumer.subscribe(listOf(inputTopic))

    val producer = KafkaProducer<String, String>(config)

    while (true) {
        val newMessages = consumer.poll(Duration.ofSeconds(10))
        newMessages.records(inputTopic).forEach {
            producer.send(ProducerRecord(outputTopic, it.value()))
        }
    }
}