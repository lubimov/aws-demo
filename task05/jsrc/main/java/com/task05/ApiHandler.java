package com.task05;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.environment.EnvironmentVariables;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.annotations.lambda.LambdaLayer;
import com.syndicate.deployment.model.ArtifactExtension;
import com.syndicate.deployment.model.DeploymentRuntime;
import com.syndicate.deployment.model.RetentionSetting;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@LambdaHandler(lambdaName = "api_handler",
		roleName = "api_handler-role",
		isPublishVersion = false,
		runtime = DeploymentRuntime.JAVA17,
		layers = {"sdk-layer"},
		logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@LambdaLayer(
        layerName = "sdk-layer",
		runtime = DeploymentRuntime.JAVA17,
        libraries = {"lib/commons-lang3-3.14.0.jar", "lib/gson-2.10.1.jar"},
        artifactExtension = ArtifactExtension.ZIP
)
@EnvironmentVariables(value = {
		@EnvironmentVariable(key = "EventsTable", value = "${target_table}")
})
// Add alias to the syndicate_aliases.yml
// target_table: Events
public class ApiHandler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {
	private LambdaLogger logger;

	private static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
	private static DynamoDB dynamoDB = new DynamoDB(client);

	private String DYNAMODB_TABLE_NAME = "FROM_ENV";
	private static final int SC_OK = 201;
	private final Gson gson = new GsonBuilder().setPrettyPrinting().create();
	private final Map<String, String> responseHeaders = Map.of("Content-Type", "application/json");

	@Override
	public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent request, Context context) {

		logger = context.getLogger();
		logger.log("RequestEvent: " + request);
		logger.log("Body: " + request.getBody());
		DYNAMODB_TABLE_NAME = System.getenv("EventsTable");
		logger.log("Events table name: " + DYNAMODB_TABLE_NAME);

		EventModel event = buildEventModelItem(request);
		insertToDynamoDbV2(event);
		return buildResponse(SC_OK, new ResponseBody(SC_OK, event));
	}

	private EventModel buildEventModelItem(APIGatewayProxyRequestEvent requestEvent){
		EventRequest requestBody = gson.fromJson(requestEvent.getBody(), EventRequest.class);
		logger.log("Parsed body: " + requestBody);

		String eventId = UUID.randomUUID().toString();
		int principalId = requestBody.principalId;
		String createdAt = Instant.now().truncatedTo(ChronoUnit.MILLIS).toString();
		Map<String, String> content = requestBody.content;
		EventModel event = new EventModel(eventId, principalId, createdAt, content);

		return event;
	}

	private void insertToDynamoDb(EventModel event){
		logger.log(">> insertToDynamoDb");
		Map<String, AttributeValue> item = new HashMap<>();
		item.put("id", new AttributeValue(event.id));
		item.put("principalId", new AttributeValue().withN(String.valueOf(event.principalId)));
		item.put("createdAt", new AttributeValue(event.createdAt));
		item.put("body", new AttributeValue().withM(buildBody(event.body())));

		PutItemRequest putItemRequest = new PutItemRequest(DYNAMODB_TABLE_NAME, item);
		logger.log("PutItemRequest: " + putItemRequest);

		PutItemResult putItemResult = client.putItem(putItemRequest);
		logger.log("PutItemResult: " + putItemResult);
	}

	private void insertToDynamoDbV2(EventModel event){
		logger.log(">> insertToDynamoDbV2");

		Table table = dynamoDB.getTable(DYNAMODB_TABLE_NAME);
		try {
			Item item = new Item()
					.withPrimaryKey("id", event.id)
					.withNumber("principalId", event.principalId)
					.withString("createdAt", event.createdAt)
					.withMap("body", event.body());

			logger.log("Item: " + item);
			PutItemOutcome result = table.putItem(item);
			logger.log("PutItemOutcome: " + result);
		} catch (Exception e) {
			System.err.println("Create items failed.");
			System.err.println(e.getMessage());

		}
	}

	private Map<String, AttributeValue> buildBody(Map<String, String> content){
		logger.log(">> buildBody. Content: " + content);
		Map<String, AttributeValue> body = new HashMap<>();

		for (Map.Entry<String, String> entry: content.entrySet()) {
			body.put(entry.getKey(), new AttributeValue(entry.getValue()));
		}

		return body;
	}

	private APIGatewayProxyResponseEvent buildResponse(int statusCode, Object body) {
		APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();
		response.setStatusCode(statusCode);
		response.setBody(gson.toJson(body));
		return response;
	}

	////// DATA MODEL
	private record EventRequest(int principalId, Map<String, String> content) {
	}

	private record EventModel(String id, int principalId, String createdAt, Map<String, String> body) {
	}

	private record ResponseBody(int statusCode, EventModel event) {
	}
}
