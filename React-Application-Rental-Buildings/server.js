const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const { Pool } = require('pg');
const app = express();

// Database connection
const pool = new Pool({
  user: ' ', //user name
  host: ' ', //ip address or localhost
  database: ' ', //database name 
  password: ' ', //database password
  port: 5432, //database port
});

// Middleware
app.use(cors());
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

// Routes
app.get('/api/data', (req, res) => {
  pool.query('select * from renters_table2', (error, results) => {
    if (error) {
      throw error;
    }
    res.status(200).json(results.rows);
  });
});


//Add 

app.post('/api/data', async (req, res) => {
  try {
    const { listing_id, status, city, state_code, postal_code, beds, baths, sqft, list_price } = req.body;
    const result = await pool.query(
      'INSERT INTO table_name (listing_id, status, city, state_code, postal_code, beds, baths, sqft, list_price) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING *',
      [listing_id, status, city, state_code, postal_code, beds, baths, sqft, list_price]
    );
    res.json(result.rows[0]);
  } catch (error) {
    console.error(error);
    res.status(500).json({ message: 'Server Error' });
  }
});


// Routes
app.delete('/api/data/:property_id', async (req, res) => {
  const { property_id } = req.params;

  try {
    const result = await pool.query('DELETE FROM renters_table2 WHERE property_id = $1', [property_id]);
    res.sendStatus(204); // successful delete, no content to send back
  } catch (error) {
    console.error(error);
    res.sendStatus(500); // internal server error
  }
});


// Start server
app.listen(3001, () => {
  console.log('Server is running on port 3001');
});
