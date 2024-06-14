package com.task04;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.syndicate.deployment.annotations.events.SqsTriggerEventSource;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.annotations.resources.DependsOn;
import com.syndicate.deployment.model.ResourceType;
import com.syndicate.deployment.model.RetentionSetting;

import java.util.*;

@SqsTriggerEventSource(targetQueue = "async_queue", batchSize = 1)
@DependsOn(name = "async_queue", resourceType = ResourceType.SQS_QUEUE)
@LambdaHandler(lambdaName = "sqs_handler",
		roleName = "sqs_handler-role",
		isPublishVersion = false,
		timeout = 100,
		logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
public class SqsHandler implements RequestHandler<SQSEvent, Map<String, Object>> {
	LambdaLogger logger;

	public Map<String, Object> handleRequest(SQSEvent event, Context context) {
		logger = context.getLogger();

		final StringBuffer buf = new StringBuffer();
		if (event != null) {
			List<SQSEvent.SQSMessage> records = event.getRecords();
			if (records != null && !records.isEmpty()) {
				Iterator<SQSEvent.SQSMessage> recordsIter = records.iterator();
				while (recordsIter.hasNext()) {
					buf.append(processRecord(recordsIter.next()));
				}
			}
		}

		System.out.println("Hello from sqs-triggered lambda");
		Map<String, Object> resultMap = new HashMap<String, Object>();
		resultMap.put("statusCode", 200);
		resultMap.put("message", buf.toString());
		return resultMap;
	}

	public SQSBatchResponse handleRequestV2(SQSEvent sqsEvent, Context context) {

		List<SQSBatchResponse.BatchItemFailure> batchItemFailures = new ArrayList<SQSBatchResponse.BatchItemFailure>();
		String messageId = "";
		for (SQSEvent.SQSMessage message : sqsEvent.getRecords()) {
			try {
				//process your message
				messageId = message.getMessageId();
			} catch (Exception e) {
				//Add failed message identifier to the batchItemFailures list
				batchItemFailures.add(new SQSBatchResponse.BatchItemFailure(messageId));
			}
		}
		return new SQSBatchResponse(batchItemFailures);
	}

	private String processRecord(SQSEvent.SQSMessage record) {
		try {
			final String message = record.getBody();
			logger.log("message: " + message);
			return message;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
