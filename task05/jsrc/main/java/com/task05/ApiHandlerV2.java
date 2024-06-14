package com.task05;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.model.DeploymentRuntime;
import com.syndicate.deployment.model.RetentionSetting;

import java.util.HashMap;
import java.util.Map;

@LambdaHandler(lambdaName = "api_handler_v2",
		roleName = "api_handler_v2-role",
		isPublishVersion = false,
		runtime = DeploymentRuntime.JAVA17,
		logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
public class ApiHandlerV2 implements RequestHandler<Object, Map<String, Object>> {
	private LambdaLogger logger;

	public Map<String, Object> handleRequest(Object request, Context context) {
		logger = context.getLogger();
		logger.log("RequestEvent: " + request);
		logger.log("Request class: " + request.getClass());

		System.out.println("Hello from lambda");
		Map<String, Object> resultMap = new HashMap<String, Object>();
		resultMap.put("statusCode", 200);
		resultMap.put("body", "Hello from Lambda");
		return resultMap;
	}
}
