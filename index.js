const app = require("./app");

// Start the Express server
const PORT = 3001;
app.listen(PORT, () => {
    console.log(`Monolith App running on port ${PORT}`);
});
