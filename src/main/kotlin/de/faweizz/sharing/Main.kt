package de.faweizz.sharing

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.time.Duration

fun main() {
    val kafkaAddress = System.getenv("KAFKA_ADDRESS") ?: throw Exception("Missing variable KAFKA_ADDRESS")
    val clientName = System.getenv("CLIENT_NAME") ?: throw Exception("Missing variable CLIENT_NAME")
    val clientSecret = System.getenv("CLIENT_SECRET") ?: throw Exception("Missing variable CLIENT_SECRET")
    val consumerGroup = System.getenv("CONSUMER_GROUP") ?: throw Exception("Missing variable CONSUMER_GROUP")
    val inputTopic = System.getenv("INPUT_TOPIC") ?: throw Exception("Missing variable INPUT_TOPIC")
    val outputTopic = System.getenv("OUTPUT_TOPIC") ?: throw Exception("Missing variable OUTPUT_TOPIC")
    val trustStoreLocation =
        System.getenv("TRUSTSTORE_LOCATION") ?: throw Exception("Missing variable TRUSTSTORE_LOCATION")
    val trustStorePassword =
        System.getenv("TRUSTSTORE_PASSWORD") ?: throw Exception("Missing variable TRUSTSTORE_PASSWORD")

    val config = Config(
        clientName = clientName,
        clientSecret = clientSecret,
        consumerGroup = consumerGroup,
        trustStoreLocation = trustStoreLocation,
        trustStorePassword = trustStorePassword,
        kafkaAddress = kafkaAddress
    )

    val consumer = KafkaConsumer<String, ByteArray>(config)
    consumer.subscribe(listOf(inputTopic))

    val producer = KafkaProducer<String, ByteArray>(config)

    while (true) {
        val newMessages = consumer.poll(Duration.ofMillis(100))
        newMessages.records(inputTopic).forEach {
            println("Received (${it.key()},${it.value()}) from $inputTopic, producing to $outputTopic")
            producer.send(ProducerRecord(outputTopic, it.value()))
        }
    }
}