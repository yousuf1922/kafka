const express = require("express");
const { createOrder } = require("./controller");

const router = express.Router();

// API endpoint for manual order creation
router.post("/order", createOrder);

module.exports = router;
