package com.task09;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.epam.openapi.OpenAPISDK;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.environment.EnvironmentVariables;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.annotations.lambda.LambdaLayer;
import com.syndicate.deployment.annotations.lambda.LambdaUrlConfig;
import com.syndicate.deployment.annotations.resources.DependsOn;
import com.syndicate.deployment.model.*;
import com.syndicate.deployment.model.lambda.url.AuthType;
import com.syndicate.deployment.model.lambda.url.InvokeMode;

import java.util.UUID;

@LambdaHandler(lambdaName = "processor",
        roleName = "processor-role",
        isPublishVersion = false,
        runtime = DeploymentRuntime.JAVA17,
        layers = {"sdk-layer"},
        tracingMode = TracingMode.Active,
        logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@LambdaLayer(
        layerName = "sdk-layer",
        runtime = DeploymentRuntime.JAVA17,
        libraries = {"lib/commons-lang3-3.14.0.jar", "lib/gson-2.10.1.jar", "lib/task08_openapi-1.1.jar"},
        artifactExtension = ArtifactExtension.ZIP
)
@LambdaUrlConfig(
        authType = AuthType.NONE,
        invokeMode = InvokeMode.BUFFERED
)
@DependsOn(name = "Weather", resourceType = ResourceType.DYNAMODB_TABLE)
@EnvironmentVariables(
        value = {
                @EnvironmentVariable(key = "target_table", value = "${target_table}")
        }
)
public class Processor implements RequestHandler<DynamodbEvent, Void> {
    private LambdaLogger logger;
    private static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
    private static DynamoDB dynamoDB = new DynamoDB(client);
    private String DYNAMODB_TABLE_NAME = "FROM_ENV";
    private final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    public Void handleRequest(DynamodbEvent dynamodbEvent, Context context) {
        logger = context.getLogger();
        logger.log("dynamodbEvent: " + dynamodbEvent.toString());
        DYNAMODB_TABLE_NAME = System.getenv("target_table");

        final OpenAPISDK apisdk = new OpenAPISDK();
        final String url = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&current=temperature_2m,wind_speed_10m&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m";
        final String apiResponse = apisdk.getForecast(url);
        logger.log("OpenAPI response: " + apiResponse);

        JsonObject forecast = JsonParser.parseString(apiResponse).getAsJsonObject();

        addItem(simplifyResponse(forecast));

        logger.log("<< handleRequest");
        return null;
    }

    private JsonObject simplifyResponse(final JsonObject forecast) {
        forecast.remove("current_units");
        forecast.remove("current");
        forecast.getAsJsonObject("hourly_units").remove("relative_humidity_2m");
        forecast.getAsJsonObject("hourly_units").remove("wind_speed_10m");
        forecast.getAsJsonObject("hourly").remove("relative_humidity_2m");
        forecast.getAsJsonObject("hourly").remove("wind_speed_10m");

        logger.log("Simplified forecast: " + forecast);
        return forecast;
    }

    private void addItem(final JsonObject forecast) {
        final String id = UUID.randomUUID().toString();
        Table table = dynamoDB.getTable(DYNAMODB_TABLE_NAME);
        try {
//			final Map<String, Object> forecastMap = new HashMap<>();
//			forecastMap.put("elevation", "N");
//			forecastMap.put("generationtime_ms", "N");
//
//			final Map<String, Object> hourlyMap = new HashMap<>();
//			forecastMap.put("temperature_2m", "N");
//			forecastMap.put("time", "S");
//			forecastMap.put("hourly", hourlyMap);
//
//			final Map<String, Object> hourlyUnitsMap = new HashMap<>();
//			forecastMap.put("temperature_2m", "N");
//			forecastMap.put("time", "S");
//			forecastMap.put("hourly_units", hourlyUnitsMap);
//
//			forecastMap.put("latitude", "N");
//			forecastMap.put("longitude", "N");
//			forecastMap.put("timezone", "S");
//			forecastMap.put("timezone_abbreviation", "S");
//			forecastMap.put("utc_offset_seconds", "N");

            Item item = new Item()
                    .withPrimaryKey("id", id)
                    .withJSON("forecast", forecast.toString());
            logger.log("Item: " + item);
            table.putItem(item);
        } catch (Exception e) {
            System.err.println("Create items failed.");
            System.err.println(e.getMessage());

        }
    }

}
