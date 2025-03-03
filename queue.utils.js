// Simple in-memory queue for background stock processing
const jobQueue = [];
let isProcessing = false;

// Function to process the stock update queue
async function processQueue() {
    if (isProcessing || jobQueue.length === 0) return;

    isProcessing = true;
    const order = jobQueue.shift();

    console.log(`Processing order: ${JSON.stringify(order)}`);

    await new Promise((resolve) => setTimeout(resolve, 2000));

    console.log(`Stock updated for item ${order.itemId}: reduced by ${order.quantity}`);

    isProcessing = false;
    processQueue();
}

// Add order to processing queue
function addToQueue(order) {
    jobQueue.push(order);
}

module.exports = {
    processQueue,
    addToQueue,
};
