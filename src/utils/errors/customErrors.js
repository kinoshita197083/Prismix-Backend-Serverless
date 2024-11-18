class AppError extends Error {
  constructor(message, statusCode = 500) {
    super(message);
    this.statusCode = statusCode;
    this.status = `${statusCode}`.startsWith("4") ? "fail" : "error";
    this.isOperational = true;

    Error.captureStackTrace(this, this.constructor);
  }
}

class S3Error extends AppError {
  constructor(message, statusCode = 500) {
    super(message, statusCode);
    this.name = "S3Error";
  }
}

class DatabaseError extends AppError {
  constructor(message, statusCode = 500) {
    super(message, statusCode);
    this.name = "DatabaseError";
  }
}

module.exports = {
  AppError,
  S3Error,
  DatabaseError,
};
