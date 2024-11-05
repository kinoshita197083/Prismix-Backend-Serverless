const { EventBridgeClient, DisableRuleCommand, DeleteRuleCommand,
    ListTargetsByRuleCommand, RemoveTargetsCommand, DescribeRuleCommand } = require("@aws-sdk/client-eventbridge");

class EventBridgeService {
    constructor(eventBridgeClient) {
        this.eventBridgeClient = eventBridgeClient;
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
}

module.exports = EventBridgeService; 