package com.task10;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.*;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.environment.EnvironmentVariables;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.annotations.lambda.LambdaLayer;
import com.syndicate.deployment.annotations.lambda.LambdaUrlConfig;
import com.syndicate.deployment.annotations.resources.Dependencies;
import com.syndicate.deployment.annotations.resources.DependsOn;
import com.syndicate.deployment.model.ArtifactExtension;
import com.syndicate.deployment.model.DeploymentRuntime;
import com.syndicate.deployment.model.ResourceType;
import com.syndicate.deployment.model.RetentionSetting;
import com.syndicate.deployment.model.lambda.url.AuthType;
import com.syndicate.deployment.model.lambda.url.InvokeMode;

import java.util.Map;
import java.util.function.Function;

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
@LambdaUrlConfig(
        authType = AuthType.NONE,
        invokeMode = InvokeMode.BUFFERED
)
@Dependencies(
        value = {
                @DependsOn(name = "Tables", resourceType = ResourceType.DYNAMODB_TABLE),
                @DependsOn(name = "Reservations", resourceType = ResourceType.DYNAMODB_TABLE)
        }
)
@EnvironmentVariables(
        value = {
                @EnvironmentVariable(key = "tables_table", value = "${target_table}"),
                @EnvironmentVariable(key = "reservations_table", value = "${reservations_table}"),
                @EnvironmentVariable(key = "cognito_userpool", value = "${booking_userpool}")
        }
)
public class ApiHandler extends AbstractRequestHandlers implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {
    private LambdaLogger logger;
    private DynamoDBHandler dynamoDBHandler;
    private AuthHandler authHandler;
    private final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    private Map<RouteKey, Function<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent>> routeHandlers;

    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent requestEvent, Context context) {
        logger = context.getLogger();
        logger.log("RequestEvent: " + requestEvent);
        logger.log("Body: " + requestEvent.getBody());

        try {
            authHandler = new AuthHandler(context);
            dynamoDBHandler = new DynamoDBHandler(context);
            initRoutes();

            RouteKey routeKey = new RouteKey(getMethod(requestEvent), getPath(requestEvent));
            logger.log("RouteKey: " + routeKey);
            return routeHandlers.getOrDefault(routeKey, this::badResponse).apply(requestEvent);
        } catch (RuntimeException e){
            return buildErrorResponse(e.getMessage());
        }
    }

    private void initRoutes(){
        routeHandlers = Map.of(
                new RouteKey("POST", "/signup"), authHandler::handleSignup,
                new RouteKey("POST", "/signin"), authHandler::handleSignin,
                new RouteKey("POST", "/tables"), dynamoDBHandler::handleTablesPost,
                new RouteKey("GET", "/tables"), dynamoDBHandler::handleTablesGet,
                new RouteKey("GET", "/tables_id"), dynamoDBHandler::handleTablesByIdGet,
                new RouteKey("POST", "/reservations"), dynamoDBHandler::handleReservationsPost,
                new RouteKey("GET", "/reservations"), dynamoDBHandler::handleReservationsGet
        );
    }

}
