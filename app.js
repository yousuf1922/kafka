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

// Connect the producer and consumer
(async function connectKafka() {
    try {
        await KafkaService.connectProducer(); 
        await KafkaService.connectConsumer(); 

        await KafkaService.subscribeTopic("orders", handleOrderMessage, true);
    } catch (error) {
        console.error("Error connecting to Kafka:", error);
    }
})();
// Scheduled task: Create a sample order every 10 seconds
cron.schedule("*/10 * * * * *", scheduledOrderCreation);

module.exports = app;
