const { AppError } = require("../utils/errors/customErrors");

class UserService {
  constructor(rdsService) {
    this.rdsService = rdsService;
  }

  async getS3Connection(userId) {
    try {
      const query = `
                SELECT s3_connection 
                FROM users 
                WHERE id = $1 AND s3_connection IS NOT NULL
            `;

      const results = await this.rdsService.query(query, [userId]);

      if (!results.length) {
        throw new AppError("S3 connection not found for user", 404);
      }

      return results[0].s3_connection;
    } catch (error) {
      if (error instanceof AppError) throw error;
      throw new AppError(
        `Failed to fetch S3 connection: ${error.message}`,
        500
      );
    }
  }
}

module.exports = { UserService };
