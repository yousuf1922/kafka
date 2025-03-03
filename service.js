const { addToQueue, processQueue } = require("./queue.utils");

function handleOrderMessage(order) {
    addToQueue(order);
    processQueue();
}

module.exports = { handleOrderMessage };
