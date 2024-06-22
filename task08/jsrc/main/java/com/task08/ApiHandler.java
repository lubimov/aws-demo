package com.task08;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.epam.openapi.OpenAPISDK;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.annotations.lambda.LambdaLayer;
import com.syndicate.deployment.annotations.lambda.LambdaUrlConfig;
import com.syndicate.deployment.model.ArtifactExtension;
import com.syndicate.deployment.model.DeploymentRuntime;
import com.syndicate.deployment.model.RetentionSetting;
import com.syndicate.deployment.model.lambda.url.AuthType;
import com.syndicate.deployment.model.lambda.url.InvokeMode;

import java.util.Map;
import java.util.function.Function;

@LambdaHandler(lambdaName = "api_handler",
		roleName = "api_handler-role",
		isPublishVersion = false,
		runtime = DeploymentRuntime.JAVA17,
		layers = {"open-apisdk-layer"},
		logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@LambdaLayer(
		layerName = "open-apisdk-layer",
		runtime = DeploymentRuntime.JAVA17,
		libraries = {"lib/task08_openapi-1.0.jar"},
		artifactExtension = ArtifactExtension.ZIP
)
@LambdaUrlConfig(
		authType = AuthType.NONE,
		invokeMode = InvokeMode.BUFFERED
)
public class ApiHandler implements RequestHandler<APIGatewayV2HTTPEvent, String> {
	private final Map<RouteKey, Function<APIGatewayV2HTTPEvent, String>> routeHandlers = Map.of(
			new RouteKey("GET", "/weather"), this::handleWeather
	);

	private String handleWeather(APIGatewayV2HTTPEvent apiGatewayV2HTTPEvent) {
		OpenAPISDK openapi = new OpenAPISDK();
		return openapi.getForecast();
	}


	@Override
	public String handleRequest(APIGatewayV2HTTPEvent requestEvent, Context context) {
		RouteKey routeKey = new RouteKey(getMethod(requestEvent), getPath(requestEvent));
		return routeHandlers.getOrDefault(routeKey, this::badResponse).apply(requestEvent);
	}

	private String badResponse(APIGatewayV2HTTPEvent apiGatewayV2HTTPEvent) {
		return "{" +
				"\"statusCode\": 404," +
				"\"message\": \"Bad request syntax or unsupported method\"" +
				"}";
	}

	private String getMethod(APIGatewayV2HTTPEvent requestEvent) {
		return requestEvent.getRequestContext().getHttp().getMethod();
	}

	private String getPath(APIGatewayV2HTTPEvent requestEvent) {
		return requestEvent.getRequestContext().getHttp().getPath();
	}

	private record RouteKey(String method, String path) {
	}
}
