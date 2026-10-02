// db.js
const { Pool } = require('pg');
require('dotenv').config();

const pool = new Pool({
  user: process.env.DB_USER || 'postgres',
  host: process.env.DB_HOST || '127.0.0.1', // Will use 'postgres' inside docker
  database: process.env.DB_NAME || 'flash_message',
  password: process.env.DB_PASSWORD || 'secretpassword',
  port: Number(process.env.DB_PORT) || 5432,
});

module.exports = pool;