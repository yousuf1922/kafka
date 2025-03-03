const { Kafka, logLevel } = require("kafkajs");

class KafkaService {
    constructor() {
        // Initialize Kafka client with host-mapped ports
        this.kafka = new Kafka({
            clientId: process.env.KAFKA_CLIENT_ID || "monolith-app",
            brokers: [
                process.env.KAFKA_BROKER_1 || "localhost:9092", // Maps to kafka-broker-1
                process.env.KAFKA_BROKER_2 || "localhost:9093", // Maps to kafka-broker-2
                process.env.KAFKA_BROKER_3 || "localhost:9094", // Maps to kafka-broker-3
            ],
            connectionTimeout: 10000, // Increase connection timeout to 10 seconds
            requestTimeout: 10000, // Increase request timeout to 10 seconds
            retry: {
                initialRetryTime: 1000, // Initial delay between retries (1 second)
                retries: 5, // Maximum number of retries per operation
                factor: 2, // Exponential backoff factor
                maxRetryTime: 30000, // Maximum total time to keep retrying (30 seconds)
            },
            logLevel: logLevel.ERROR, // Set log level to ERROR to reduce verbosity
        });

        // Initialize producer, consumer, and admin instances
        this.producer = this.kafka.producer();
        this.consumer = this.kafka.consumer({ groupId: process.env.KAFKA_CONSUMER_GROUP_ID || "stock-group" });
        this.admin = this.kafka.admin();
        this.isProducerConnected = false;
        this.isConsumerConnected = false;
    }

    async connectProducer() {
        try {
            await this.producer.connect();
            this.isProducerConnected = true;
            console.log("Kafka Producer connected");
        } catch (error) {
            this.isProducerConnected = false;
            console.error("Error connecting Kafka Producer:", error);
            throw error;
        }
    }

    async disconnectProducer() {
        try {
            await this.producer.disconnect();
            this.isProducerConnected = false;
            console.log("Kafka Producer disconnected");
        } catch (error) {
            console.error("Error disconnecting Kafka Producer:", error);
            throw error;
        }
    }

    async connectConsumer() {
        try {
            await this.consumer.connect();
            this.isConsumerConnected = true;
            console.log("Kafka Consumer connected");
        } catch (error) {
            this.isConsumerConnected = false;
            console.error("Error connecting Kafka Consumer:", error);
            throw error;
        }
    }

    async disconnectConsumer() {
        try {
            await this.consumer.disconnect();
            this.isConsumerConnected = false;
            console.log("Kafka Consumer disconnected");
        } catch (error) {
            console.error("Error disconnecting Kafka Consumer:", error);
            throw error;
        }
    }

    // Method to send a message to a Kafka topic
    async publishMessage(topic, message, autoDisconnect = false) {
        try {
            if (!this.isProducerConnected) {
                console.log("Producer not connected, connecting automatically...");
                await this.connectProducer();
            }
            await this.producer.send({
                topic,
                messages: [{ value: JSON.stringify(message) }],
            });
            console.log(`Message published to ${topic}: ${JSON.stringify(message)}`);
            if (autoDisconnect) {
                await this.disconnectProducer();
            }
        } catch (error) {
            console.error(`Error publishing message to ${topic}:`, error);
            throw error; // KafkaJS retry will handle retries internally
        }
    }

    // Method to subscribe to a topic and process messages with a callback
    async subscribeTopic(topic, callback, fromBeginning = true) {
        try {
            if (!this.isConsumerConnected) {
                console.log("Consumer not connected, connecting automatically...");
                await this.connectConsumer();
            }
            await this.consumer.subscribe({ topic, fromBeginning });
            await this.consumer.run({
                eachMessage: async ({ topic, partition, message }) => {
                    const data = JSON.parse(message.value.toString());
                    console.log(`Received message from ${topic}: ${JSON.stringify(data)}`);
                    await callback(data);
                },
            });
        } catch (error) {
            console.error(`Error subscribing to ${topic}:`, error);
            throw error; // KafkaJS retry will handle retries internally
        }
    }

    async createTopic(topic, numPartitions = 1, replicationFactor = 1) {
        try {
            await this.admin.connect();
            await this.admin.createTopics({
                topics: [
                    {
                        topic,
                        numPartitions,
                        replicationFactor,
                    },
                ],
            });
            console.log(`Topic ${topic} created successfully`);
        } catch (error) {
            console.error(`Error creating topic ${topic}:`, error);
            throw error;
        } finally {
            await this.admin.disconnect();
        }
    }

    // getAdmin method to return a connected admin client
    async getAdmin() {
        if (!this.admin._isConnected) {
            // Check if admin is already connected (internal property check)
            try {
                await this.admin.connect();
                console.log("Kafka Admin connected");
            } catch (error) {
                console.error("Error connecting Kafka Admin:", error);
                throw error;
            }
        }
        // Return a proxy to automatically disconnect after use
        return {
            listTopics: async () => {
                try {
                    return await this.admin.listTopics();
                } finally {
                    await this.admin.disconnect();
                    console.log("Kafka Admin disconnected after listTopics");
                }
            },
            createTopics: async (config) => {
                try {
                    return await this.admin.createTopics(config);
                } finally {
                    await this.admin.disconnect();
                    console.log("Kafka Admin disconnected after createTopics");
                }
            },
            // Add more admin methods as needed
        };
    }
}

module.exports = {
    KafkaService: new KafkaService(),
};
