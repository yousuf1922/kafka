const { KafkaService } = require("./kafkaService");

async function createOrder(req, res) {
    const order = {
        orderId: Date.now(),
        itemId: req.body.itemId,
        quantity: req.body.quantity,
    };
    try {
        await KafkaService.publishMessage("orders", order);
        res.status(201).send({ message: "Order created", order });
    } catch (error) {
        console.error("Error creating order:", error);
        res.status(500).send({ message: "Failed to create order" });
    }
}

module.exports = { createOrder };
