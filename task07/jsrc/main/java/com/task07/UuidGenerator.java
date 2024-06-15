package com.task07;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.environment.EnvironmentVariables;
import com.syndicate.deployment.annotations.events.RuleEventSource;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.annotations.lambda.LambdaLayer;
import com.syndicate.deployment.model.ArtifactExtension;
import com.syndicate.deployment.model.DeploymentRuntime;
import com.syndicate.deployment.model.RetentionSetting;
import com.syndicate.deployment.model.events.RuleEventSourceItem;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

@RuleEventSource(targetRule = "uuid_trigger")
@LambdaHandler(lambdaName = "uuid_generator",
		roleName = "uuid_generator-role",
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
@EnvironmentVariables(
		value = {
				@EnvironmentVariable(key = "target_bucket", value = "${target_bucket}")
		}
)
public class UuidGenerator implements RequestHandler<RuleEventSourceItem, Void> {
	private LambdaLogger logger;
	private String s3BucketName;
	private Regions clientRegion = Regions.EU_CENTRAL_1;
	private final Gson gson = new GsonBuilder().setPrettyPrinting().create();

	public Void handleRequest(RuleEventSourceItem event, Context context) {
		logger = context.getLogger();
		logger.log("event: " + event.toString());

		final AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
				.withRegion(clientRegion)
				.build();

		s3BucketName = System.getenv("target_bucket");

		final String fileName = Instant.now().truncatedTo(ChronoUnit.MILLIS).toString();

		try  {
			// Upload a file as a new object
			byte[] bytes = generateFileContent().getBytes(StandardCharsets.UTF_8);
			ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);

			ObjectMetadata metadata = new ObjectMetadata();
			metadata.setContentType("application/json");
			metadata.setContentLength(bytes.length);

			s3Client.putObject(s3BucketName, fileName, byteArrayInputStream, metadata);
		} catch (AmazonServiceException e) {
			// The call was transmitted successfully, but Amazon S3 couldn't process
			// it, so it returned an error response.
			logger.log("ERROR AmazonServiceException: " + e.getMessage());
		} catch (SdkClientException e) {
			// Amazon S3 couldn't be contacted for a response, or the client
			// couldn't parse the response from Amazon S3.
			logger.log("ERROR SdkClientException: " + e.getMessage());
		}
		return null;
	}

	private String generateFileContent(){
		String[] ids = new String[10];

		for (int i = 0; i < 10; i++) {
			ids[i] = UUID.randomUUID().toString();
		}

		return gson.toJson(new Content(ids));
	}

	private record Content(String[] ids) {
	}
}
