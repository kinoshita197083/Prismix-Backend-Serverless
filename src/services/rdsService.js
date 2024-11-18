const { Client } = require("pg");
const { AppError } = require("../utils/errors/customErrors");

class RDSService {
  constructor() {
    this.client = new Client({
      host: process.env.RDS_HOST,
      port: process.env.RDS_PORT,
      database: process.env.RDS_DATABASE,
      user: process.env.RDS_USERNAME,
      password: process.env.RDS_PASSWORD,
      ssl: {
        rejectUnauthorized: false,
      },
    });
  }

  async connect() {
    if (!this.client._connected) {
      await this.client.connect();
    }
  }

  async disconnect() {
    if (this.client._connected) {
      await this.client.end();
    }
  }

  async query(sql, params = []) {
    try {
      await this.connect();
      const result = await this.client.query(sql, params);
      return result.rows;
    } catch (error) {
      throw new AppError(`Database query failed: ${error.message}`, 500);
    } finally {
      await this.disconnect();
    }
  }
}

module.exports = { RDSService };
