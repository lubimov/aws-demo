package com.task04;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.syndicate.deployment.annotations.events.SnsEventSource;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.annotations.resources.DependsOn;
import com.syndicate.deployment.model.ResourceType;
import com.syndicate.deployment.model.RetentionSetting;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@SnsEventSource(targetTopic = "lambda_topic")
@DependsOn(name = "lambda_topic", resourceType = ResourceType.SNS_TOPIC)
@LambdaHandler(lambdaName = "sns_handler",
		roleName = "sns_handler-role",
		isPublishVersion = false,
		timeout = 100,
		logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
public class SnsHandler implements RequestHandler<SNSEvent, Map<String, Object>> {
	LambdaLogger logger;

	public Map<String, Object> handleRequest(SNSEvent event, Context context) {
		logger = context.getLogger();

		final StringBuffer buf = new StringBuffer();
		if (event != null) {
			List<SNSEvent.SNSRecord> records = event.getRecords();
			if (records != null && !records.isEmpty()) {
				Iterator<SNSEvent.SNSRecord> recordsIter = records.iterator();
				while (recordsIter.hasNext()) {
					buf.append(processRecord(recordsIter.next()));
				}
			}
		}

		System.out.println("Hello from sns-triggered lambda");
		Map<String, Object> resultMap = new HashMap<String, Object>();
		resultMap.put("statusCode", 200);
		resultMap.put("message", buf.toString());
		return resultMap;
	}

	private String processRecord(SNSEvent.SNSRecord record) {
		try {
			final String message = record.getSNS().getMessage();
			logger.log("message: " + message);
			return message;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
