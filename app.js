const express = require("express");
const cron = require("node-cron");
const route = require("./route");
const { KafkaService } = require("./kafkaService");
const { scheduledOrderCreation } = require("./schedule");
const { handleOrderMessage } = require("./service");

const app = express();

// Middleware
app.use(express.json());

// Register routes
app.use(route);

// // Connect the producer and consumer
// (async function connectKafka() {
//     try {
//         await KafkaService.connectProducer(); 
//         await KafkaService.connectConsumer(); 

//         await KafkaService.subscribeTopic("orders", handleOrderMessage, true);
//     } catch (error) {
//         console.error("Error connecting to Kafka:", error);
//     }
// })();
// // Scheduled task: Create a sample order every 10 seconds
// cron.schedule("*/10 * * * * *", scheduledOrderCreation);

// Initialize Kafka with producer, consumer, and topic subscription
(async function initializeKafka() {
    try {
        // Connect Producer
        await KafkaService.connectProducer();

        // Ensure the 'orders' topic exists
        await KafkaService.createTopic("orders", 6, 3);

        // Connect Consumer and Subscribe
        await KafkaService.connectConsumer();

        await KafkaService.subscribeTopic("orders", handleOrderMessage, true);
    } catch (error) {
        console.error("Failed to initialize Kafka:", error);
        process.exit(1); // Exit if Kafka setup fails
    }
})();

// Scheduled task: Create a sample order every 10 seconds
cron.schedule("*/10 * * * * *", scheduledOrderCreation);

// Graceful shutdown
process.on("SIGTERM", async () => {
    console.log("Shutting down...");
    await KafkaService.disconnectConsumer();
    await KafkaService.disconnectProducer();
    process.exit(0);
});

module.exports = app;
