const { DisableRuleCommand, DeleteRuleCommand,
    DescribeRuleCommand, PutRuleCommand, PutTargetsCommand, ListTargetsByRuleCommand, RemoveTargetsCommand } = require("@aws-sdk/client-eventbridge");

class EventBridgeService {
    constructor(eventBridgeClient, config) {
        this.eventBridgeClient = eventBridgeClient;
        this.config = config;
    }

    async disableAndDeleteRule(jobId, ruleName) {
        try {
            console.log(`[EventBridgeService] Cleaning up rule: ${ruleName}`);

            try {
                await this.eventBridgeClient.send(new DescribeRuleCommand({
                    Name: ruleName
                }));
                console.log(`[EventBridgeService] Rule ${ruleName} exists, proceeding with cleanup`);
            } catch (error) {
                if (error.name === 'ResourceNotFoundException') {
                    console.log(`[EventBridgeService] Rule ${ruleName} doesn't exist, skipping cleanup`);
                    return;
                }
                console.error(`[EventBridgeService] Error describing rule ${ruleName}:`, error);
                throw error;
            }

            // Disable and remove targets before deleting the rule
            await Promise.all([
                this._disableRule(ruleName),
                this._removeTargets(ruleName),
            ]);

            // Delete the rule
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
            console.log('[EventBridgeService] Deleted EventBridge rule:', ruleName);
        } catch (error) {
            if (error.name !== 'ResourceNotFoundException') {
                throw error;
            }
        }
    }

    async _removeTargets(ruleName) {
        try {
            // First, get all targets for the rule
            const listTargetsParams = {
                Rule: ruleName,
                EventBusName: process.env.EVENT_BUS_NAME || 'default'
            };

            const targets = await this.eventBridgeClient.send(new ListTargetsByRuleCommand(listTargetsParams));

            if (targets.Targets && targets.Targets.length > 0) {
                // Remove all targets
                const targetIds = targets.Targets.map(target => target.Id);
                await this.eventBridgeClient.send(new RemoveTargetsCommand({
                    Rule: ruleName,
                    EventBusName: process.env.EVENT_BUS_NAME || 'default',
                    Ids: targetIds
                }));
            }
        } catch (error) {
            console.error(`[EventBridgeService] Error removing targets for rule ${ruleName}:`, error);
            throw error;
        }
    }

    async createEventBridgeRule(ruleName, scheduleExpression, jobId, action, status) {
        console.log(`[EventBridgeService] Creating rule with full config:`, {
            ruleName,
            scheduleExpression,
            lambdaArn: this.config.lambdaArn,
            targetId: `${action}Target-${jobId}`,
            inputPayload: JSON.stringify({
                Records: [{
                    messageId: `${action}-${Date.now()}`,
                    body: JSON.stringify({
                        jobId,
                        action,
                        status,
                        timestamp: Date.now().toString()
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
        });

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
                                timestamp: Date.now().toString()
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