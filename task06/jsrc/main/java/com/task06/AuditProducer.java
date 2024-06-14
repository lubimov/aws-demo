package com.task06;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.environment.EnvironmentVariables;
import com.syndicate.deployment.annotations.events.DynamoDbTriggerEventSource;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.annotations.resources.DependsOn;
import com.syndicate.deployment.model.DeploymentRuntime;
import com.syndicate.deployment.model.ResourceType;
import com.syndicate.deployment.model.RetentionSetting;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@LambdaHandler(lambdaName = "audit_producer",
		roleName = "audit_producer-role",
		isPublishVersion = false,
		runtime = DeploymentRuntime.JAVA17,
		logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@DependsOn(name = "Configuration", resourceType = ResourceType.DYNAMODB_TABLE)
@DependsOn(name = "Audit", resourceType = ResourceType.DYNAMODB_TABLE)
@DynamoDbTriggerEventSource(targetTable = "Configuration", batchSize = 1)
@EnvironmentVariables(
		value = {
				@EnvironmentVariable(key = "target_table", value = "${target_table}")
		}
)
public class AuditProducer implements RequestHandler<DynamodbEvent, Void> {
	private LambdaLogger logger;
	private static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
	private static DynamoDB dynamoDB = new DynamoDB(client);
	private String DYNAMODB_TABLE_NAME = "FROM_ENV";

	public Void handleRequest(DynamodbEvent dynamodbEvent, Context context) {
		logger = context.getLogger();
		logger.log("dynamodbEvent: " + dynamodbEvent.toString());
		DYNAMODB_TABLE_NAME = System.getenv("target_table");

		dynamodbEvent.getRecords().stream().forEachOrdered(record -> {
			switch (record.getEventName()) {
				case "INSERT":
					auditInsert(record);
					break;
				case "MODIFY":
					auditModify(record);
					break;
				default:
					logger.log("ERROR: Unknown event: " + record.getEventName());
			}
		});

		return null;
	}

	private void auditInsert(DynamodbEvent.DynamodbStreamRecord record) {
		logger.log(">> auditInsert");
		Map<String, AttributeValue> newImage = record.getDynamodb().getNewImage();

		final Map<String, Object> newValue = new HashMap<>();
		newValue.put("key", newImage.get("key").getS());
		newValue.put("value", Integer.valueOf(newImage.get("value").getN()));
		final String id = UUID.randomUUID().toString();
		final String modificationTime = Instant.now().truncatedTo(ChronoUnit.MILLIS).toString();

		Table table = dynamoDB.getTable(DYNAMODB_TABLE_NAME);
		try {
			Item item = new Item()
					.withPrimaryKey("id", id)
					.withString("itemKey", newImage.get("key").getS())
					.withString("modificationTime", modificationTime)
					.withMap("newValue", newValue);
			logger.log("Item: " + item);
			table.putItem(item);
		} catch (Exception e) {
			System.err.println("Create items failed.");
			System.err.println(e.getMessage());

		}
	}

	private void auditModify(DynamodbEvent.DynamodbStreamRecord record) {
		logger.log(">> auditModify");
		Map<String, AttributeValue> oldImage = record.getDynamodb().getOldImage();
		Map<String, AttributeValue> newImage = record.getDynamodb().getNewImage();

		final String id = UUID.randomUUID().toString();
		final String modificationTime = Instant.now().truncatedTo(ChronoUnit.MILLIS).toString();

		Table table = dynamoDB.getTable(DYNAMODB_TABLE_NAME);
		try {
			Item item = new Item()
					.withPrimaryKey("id", id)
					.withString("itemKey", newImage.get("key").getS())
					.withString("modificationTime", modificationTime)
					.withString("updatedAttribute", "value")
					.withInt("oldValue", Integer.valueOf(oldImage.get("value").getN()))
					.withInt("newValue", Integer.valueOf(newImage.get("value").getN()));

			logger.log("Item: " + item);
			PutItemOutcome result = table.putItem(item);
			logger.log("PutItemOutcome: " + result);
		} catch (Exception e) {
			System.err.println("Create items failed.");
			System.err.println(e.getMessage());
		}
	}

}

