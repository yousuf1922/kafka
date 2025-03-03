const { KafkaService } = require("./kafkaService");


async function scheduledOrderCreation() {
    const scheduledOrder = {
        orderId: Date.now(),
        itemId: `item-${Math.floor(Math.random() * 100)}`,
        quantity: Math.floor(Math.random() * 5) + 1,
    };
    console.log("Scheduled order creation");
    try {
        await KafkaService.publishMessage("orders", scheduledOrder);
    } catch (error) {
        console.error("Error in scheduled order creation:", error);
    }
}

module.exports = { scheduledOrderCreation };
