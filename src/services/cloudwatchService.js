const { CloudWatchClient, PutMetricDataCommand } = require('@aws-sdk/client-cloudwatch');


class CloudWatchService {
    constructor(cloudWatchClient, config) {
        this.cloudWatchClient = cloudWatchClient;
    }

    async recordMetrics(operationName, metrics) {
        try {
            // Create CloudWatch metrics
            const timestamp = new Date();
            const dimensions = [
                { Name: 'Operation', Value: operationName },
                { Name: 'Environment', Value: process.env.STAGE || 'development' }
            ];

            // Validate dimensions
            if (!dimensions.every(d => d.Value)) {
                console.warn('Missing dimension values:', dimensions);
                return; // Skip metric recording if dimensions are invalid
            }

            const metricData = [
                {
                    MetricName: 'OperationDuration',
                    Value: metrics.duration || 0,
                    Unit: 'Milliseconds',
                    Timestamp: timestamp,
                    Dimensions: dimensions
                },
                {
                    MetricName: 'OperationSuccess',
                    Value: metrics.success ? 1 : 0,
                    Unit: 'Count',
                    Timestamp: timestamp,
                    Dimensions: dimensions
                }
            ];

            if (!metrics.success && metrics.errorCode) {
                metricData.push({
                    MetricName: 'OperationError',
                    Value: 1,
                    Unit: 'Count',
                    Timestamp: timestamp,
                    Dimensions: [
                        ...dimensions,
                        {
                            Name: 'ErrorCode',
                            Value: metrics.errorCode || 'UNKNOWN_ERROR'
                        }
                    ]
                });
            }

            await this.cloudWatchClient.send(new PutMetricDataCommand({
                Namespace: 'JobProgress',
                MetricData: metricData.filter(metric =>
                    metric.Dimensions.every(d => d.Value != null && d.Value !== '')
                )
            }));
        } catch (error) {
            console.error('Error recording metrics:', {
                error: error.message,
                operationName,
                metrics
            });
            // Don't throw - metrics recording should not affect main flow
        }
    }

    async withPerformanceTracking(operationName, fn) {
        const startTime = Date.now();
        try {
            const result = await fn();
            const duration = Date.now() - startTime;

            // Log performance metrics
            if (duration > PERFORMANCE_METRICS.SLOW_QUERY_THRESHOLD) {
                console.warn('Slow operation detected:', {
                    operation: operationName,
                    duration,
                    threshold: PERFORMANCE_METRICS.SLOW_QUERY_THRESHOLD
                });
            }

            // Track metrics
            await this.recordMetrics(operationName, {
                duration,
                success: true
            });

            return result;
        } catch (error) {
            const duration = Date.now() - startTime;

            // Track error metrics
            await this.recordMetrics(operationName, {
                duration,
                success: false,
                errorCode: error.code
            });

            throw error;
        }
    }
}

module.exports = CloudWatchService;