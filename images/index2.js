const express = require('express');
const mongoose = require('mongoose');
const multer = require('multer');
const path = require('path');
const amqp = require('amqplib');

const app = express();
const PORT = process.env.PORT || 3000;

// Connect to MongoDB
mongoose.connect('mongodb://localhost:27017/imageUploader', {
  useNewUrlParser: true,
  useUnifiedTopology: true,
});

// Create a Mongoose model for storing images
const Image = mongoose.model('Image', {
  filename: String,
  originalname: String,
  path: String,
  processedDataId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: 'ProcessedData',
  },
});

// Create a Mongoose model for storing processed data
const ProcessedData = mongoose.model('ProcessedData', {
  imageId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: 'Image',
  },
  // Add fields for your tabular data structure
  // Example: dataField1: String,
  //          dataField2: Number,
  //          ...
});

// Set up Multer for file uploads
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    cb(null, 'uploads/');
  },
  filename: (req, file, cb) => {
    cb(null, Date.now() + path.extname(file.originalname));
  },
});

const upload = multer({ storage: storage });

// RabbitMQ connection URL
const rabbitmqURL = 'amqp://localhost';

// RabbitMQ connection and channel creation
async function createChannel() {
  const connection = await amqp.connect(rabbitmqURL);
  const channel = await connection.createChannel();
  return channel;
}

// Send image data to the data team via RabbitMQ                                            // how data team gonna use this endpoint ?.....................................
async function sendToDataTeam(image) {
  const channel = await createChannel();
  const queueName = 'dataProcessingQueue';

  await channel.assertQueue(queueName, { durable: false });
  channel.sendToQueue(queueName, Buffer.from(JSON.stringify(image)));

  console.log('Image sent to the data team for processing');
}

// Receive processed data from the data team via RabbitMQ                                  
async function receiveProcessedData() {
  const channel = await createChannel();
  const queueName = 'processedDataQueue';

  await channel.assertQueue(queueName, { durable: false });

  channel.consume(queueName, async (msg) => {
    const processedData = JSON.parse(msg.content.toString());
    console.log('Received processed data:', processedData);

    // Store processed data in MongoDB
    await saveProcessedData(processedData);

    // Acknowledge the message
    channel.ack(msg);
  });
}

// Save processed data to MongoDB
async function saveProcessedData(processedData) {
  const newProcessedData = new ProcessedData(processedData);
  await newProcessedData.save();

  // Update the image document with the processed data ID
  await Image.findByIdAndUpdate(processedData.imageId, {
    $set: { processedDataId: newProcessedData._id },
  });
}

// Start listening for processed data
receiveProcessedData();

// Upload image endpoint
app.post('/upload', upload.single('image'), async (req, res) => {
  try {
    const { filename, originalname, path } = req.file;
    const image = new Image({ filename, originalname, path });
    await image.save();

    // Send the image to RabbitMQ for data processing
    await sendToDataTeam({
      imageId: image._id,
      imagePath: image.path,
    });

    res.status(201).json({ message: 'Image uploaded successfully' });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

// Get all images endpoint
app.get('/images', async (req, res) => {
  try {
    const images = await Image.find();
    res.status(200).json(images);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

// Get a specific image by ID and its processed data endpoint
app.get('/images/:id', async (req, res) => {
  try {
    const image = await Image.findById(req.params.id);
    if (!image) {
      return res.status(404).json({ error: 'Image not found' });
    }

    // Fetch the processed data associated with the image
    const processedData = await ProcessedData.findOne({ imageId: image._id });

    // Combine image and processed data in the response
    const response = {
      image,
      processedData,
    };

    res.status(200).json(response);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

// Get all processed data endpoint
app.get('/processed-data', async (req, res) => {
  try {
    const processedData = await ProcessedData.find();
    res.status(200).json(processedData);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});
