const { EventBridgeClient, DisableRuleCommand, DeleteRuleCommand,
    ListTargetsByRuleCommand, RemoveTargetsCommand } = require("@aws-sdk/client-eventbridge");

class EventBridgeService {
    constructor(eventBridgeClient) {
        this.eventBridgeClient = eventBridgeClient;
    }

    async disableAndDeleteRule(jobId) {
        const ruleName = `JobProgressCheck-${jobId}`;
        console.log('[EventBridgeService.disableAndDeleteRule] Starting cleanup for rule:', ruleName);

        try {
            await this._disableRule(ruleName);
            await this._removeTargets(ruleName);
            await this._deleteRule(ruleName);
            console.log('[EventBridgeService.disableAndDeleteRule] Cleanup completed successfully');
        } catch (error) {
            console.error('[EventBridgeService.disableAndDeleteRule] Error during cleanup:', error);
            throw error;
        }
    }

    async _disableRule(ruleName) {
        console.log('[EventBridgeService._disableRule] Disabling rule:', ruleName);
        await this.eventBridgeClient.send(new DisableRuleCommand({ Name: ruleName }));
        console.log('[EventBridgeService._disableRule] Rule disabled successfully');
    }

    async _removeTargets(ruleName) {
        console.log('[EventBridgeService._removeTargets] Removing targets for rule:', ruleName);

        const listTargetsResponse = await this.eventBridgeClient.send(
            new ListTargetsByRuleCommand({ Rule: ruleName })
        );
        console.log('[EventBridgeService._removeTargets] Found targets:', listTargetsResponse.Targets);

        if (listTargetsResponse.Targets?.length > 0) {
            await this.eventBridgeClient.send(new RemoveTargetsCommand({
                Rule: ruleName,
                Ids: listTargetsResponse.Targets.map(target => target.Id)
            }));
            console.log('[EventBridgeService._removeTargets] Targets removed successfully');
        }
    }

    async _deleteRule(ruleName) {
        console.log('[EventBridgeService._deleteRule] Deleting rule:', ruleName);
        await this.eventBridgeClient.send(new DeleteRuleCommand({ Name: ruleName }));
        console.log('[EventBridgeService._deleteRule] Rule deleted successfully');
    }
}

module.exports = EventBridgeService; 