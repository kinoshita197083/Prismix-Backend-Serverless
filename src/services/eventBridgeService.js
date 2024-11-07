const { DisableRuleCommand, DeleteRuleCommand,
    DescribeRuleCommand, PutRuleCommand, PutTargetsCommand } = require("@aws-sdk/client-eventbridge");

class EventBridgeService {
    constructor(eventBridgeClient, config) {
        this.eventBridgeClient = eventBridgeClient;
        this.config = config;
    }

    getRuleName(jobId) {
        if (jobId.startsWith('JobProgressCheck-')) {
            return jobId;
        }
        const ruleName = `JobProgressCheck-${jobId}`;
        return ruleName.substring(0, 64);
    }

    async disableAndDeleteRule(jobId) {
        try {
            const ruleName = this.getRuleName(jobId);
            console.log(`[EventBridgeService] Cleaning up rule: ${ruleName}`);

            try {
                await this.eventBridgeClient.send(new DescribeRuleCommand({
                    Name: ruleName
                }));
            } catch (error) {
                if (error.name === 'ResourceNotFoundException') {
                    console.log(`[EventBridgeService] Rule ${ruleName} doesn't exist, skipping cleanup`);
                    return;
                }
                throw error;
            }

            await this._disableRule(ruleName);
            await this._deleteRule(ruleName);

        } catch (error) {
            console.error('[EventBridgeService] Error in disableAndDeleteRule:', error);
            throw error;
        }
    }

    async _disableRule(ruleName) {
        try {
            console.log(`[EventBridgeService] Disabling rule: ${ruleName}`);
            await this.eventBridgeClient.send(new DisableRuleCommand({
                Name: ruleName
            }));
        } catch (error) {
            if (error.name !== 'ResourceNotFoundException') {
                throw error;
            }
        }
    }

    async _deleteRule(ruleName) {
        try {
            console.log(`[EventBridgeService] Deleting rule: ${ruleName}`);
            await this.eventBridgeClient.send(new DeleteRuleCommand({
                Name: ruleName
            }));
        } catch (error) {
            if (error.name !== 'ResourceNotFoundException') {
                throw error;
            }
        }
    }

    async createEventBridgeRule(ruleName, scheduleExpression, jobId, action, status) {
        console.log(`[EventBridgeService] Creating rule: ${ruleName} with schedule: ${scheduleExpression} & config: `, this.config);
        try {
            await this.eventBridgeClient.send(new PutRuleCommand({
                Name: ruleName,
                ScheduleExpression: scheduleExpression,
                State: 'ENABLED',
                Description: `${action} schedule for job ${jobId}`
            }));

            await this.eventBridgeClient.send(new PutTargetsCommand({
                Rule: ruleName,
                Targets: [{
                    Id: `${action}Target-${jobId}`,
                    Arn: this.config.lambdaArn,
                    Input: JSON.stringify({
                        Records: [{
                            messageId: `${action}-${Date.now()}`,
                            body: JSON.stringify({
                                jobId,
                                action,
                                status,
                                timestamp: new Date().toISOString()
                            }),
                            messageAttributes: {
                                eventType: {
                                    stringValue: action,
                                    stringListValues: [],
                                    binaryListValues: [],
                                    dataType: "String"
                                }
                            }
                        }]
                    })
                }]
            }));

            console.log(`Scheduled ${action} for job ${jobId}`);
            return true;
        } catch (error) {
            console.error(`Error scheduling ${action}:`, error);
            return false;
        }
    };
}

module.exports = EventBridgeService; 