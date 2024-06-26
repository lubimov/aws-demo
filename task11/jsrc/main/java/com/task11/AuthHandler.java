package com.task11;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.services.cognitoidentityprovider.CognitoIdentityProviderClient;
import software.amazon.awssdk.services.cognitoidentityprovider.model.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class AuthHandler extends AbstractRequestHandlers {
    public static final String TEMP_PASSWORD = "TempPassword123!";
    private LambdaLogger logger;
    private String cognitoUserPoolName;
    private String cognitoUserPoolId;
    private String cognitoClientId;

    private static final String CLIENT_APP = "client-app";

    CognitoIdentityProviderClient cognitoClient = CognitoIdentityProviderClient.create();


    public AuthHandler(Context context) {
        logger = context.getLogger();
        cognitoUserPoolName = System.getenv("cognito_userpool");

        cognitoUserPoolId = getUserPoolId();
        cognitoClientId = getClientId();
    }

    public APIGatewayProxyResponseEvent handleSignin(APIGatewayProxyRequestEvent requestEvent) {
        logger.log(">> handleSignin");
        try {
            final SinginRecord request =
                    gson.fromJson(requestEvent.getBody(), SinginRecord.class);

            Map<String, String> authParameters = new HashMap<>();
            authParameters.put("USERNAME", request.email);
            authParameters.put("PASSWORD", request.password);

            AdminInitiateAuthRequest authRequest = AdminInitiateAuthRequest.builder()
                    .authFlow(AuthFlowType.ADMIN_USER_PASSWORD_AUTH)
                    .clientId(cognitoClientId)
                    .userPoolId(cognitoUserPoolId)
                    .authParameters(authParameters)
                    .build();

            AdminInitiateAuthResponse authResponse = cognitoClient.adminInitiateAuth(authRequest);
            String accessToken = authResponse.authenticationResult().idToken(); //.accessToken();

            Map<String, String> response = Map.of("accessToken", accessToken);
            return buildResponse(SC_OK, gson.toJson(response));
        } catch (Exception e) {
            logger.log("ERROR: %s".formatted(e.getMessage()));
            logger.log("ERROR: %s".formatted(Arrays.asList(e.getStackTrace())));
            return buildErrorResponse(e.getMessage());
        }
    }

    public APIGatewayProxyResponseEvent handleSignup(APIGatewayProxyRequestEvent requestEvent) {
        logger.log(">> handleSignup");
        final UserRecord request = gson.fromJson(requestEvent.getBody(), UserRecord.class);

        if (!isValidSignupRequest(request)) {
            return buildErrorResponse("Invalid signup request. All fields must be non-empty.");
        }

        // Validate the password
        if (!isValidPassword(request.password)) {
            return buildErrorResponse("Invalid password. The password must be alphanumeric, " +
                    "include at least one of the special characters $%^*, and be at least 12 characters long.");
        }

        try {
            AdminCreateUserRequest createUserRequest = AdminCreateUserRequest.builder()
                    .userPoolId(cognitoUserPoolId)
                    .username(request.email)
                    .userAttributes(
                            AttributeType.builder()
                                    .name("email")
                                    .value(request.email)
                                    .build(),
                            AttributeType.builder()
                                    .name("family_name")
                                    .value(request.firstName)
                                    .build(),
                            AttributeType.builder()
                                    .name("given_name")
                                    .value(request.lastName)
                                    .build())
                    .temporaryPassword(TEMP_PASSWORD)
                    .messageAction(MessageActionType.SUPPRESS)
                    .build();

            AdminCreateUserResponse createUserResponse = cognitoClient.adminCreateUser(createUserRequest);

            Thread.sleep(5000);

            // sign in
            Map<String, String> authParameters = new HashMap<>();
            authParameters.put("USERNAME", request.email);
            authParameters.put("PASSWORD", TEMP_PASSWORD);

            AdminInitiateAuthRequest initialRequest = AdminInitiateAuthRequest.builder()
                    .authFlow(AuthFlowType.ADMIN_NO_SRP_AUTH)
                    .authParameters(authParameters)
                    .clientId(cognitoClientId)
                    .userPoolId(cognitoUserPoolId)
                    .build();

            AdminInitiateAuthResponse initialResponse = cognitoClient.adminInitiateAuth(initialRequest);
            if (ChallengeNameType.NEW_PASSWORD_REQUIRED != initialResponse.challengeName()) {
                throw new RuntimeException("Unexpected challenge: %s".formatted(initialResponse.challengeName()));
            }

            // Set user's permanent password
            Map<String, String> challengeResponses = new HashMap<>();
            challengeResponses.put("USERNAME", request.email);
            challengeResponses.put("PASSWORD", TEMP_PASSWORD);
            challengeResponses.put("NEW_PASSWORD", request.password);

            AdminRespondToAuthChallengeRequest finalRequest = AdminRespondToAuthChallengeRequest.builder()
                    .challengeName(ChallengeNameType.NEW_PASSWORD_REQUIRED)
                    .challengeResponses(challengeResponses)
                    .clientId(cognitoClientId)
                    .userPoolId(cognitoUserPoolId)
                    .session(initialResponse.session())
                    .build();

            AdminRespondToAuthChallengeResponse response = cognitoClient.adminRespondToAuthChallenge(finalRequest);
            if (response.challengeName() != null) {
                throw new RuntimeException("Unexpected challenge: %s".formatted(initialResponse.challengeName()));
            }

            return buildResponse(SC_OK, "");
        } catch (Exception e) {
            logger.log("ERROR: %s".formatted(e.getMessage()));
            logger.log("ERROR: %s".formatted(Arrays.asList(e.getStackTrace())));
            return buildErrorResponse(e.getMessage());
        }
    }

    private String getUserPoolId() {
        String userPoolId = cognitoClient.listUserPools(ListUserPoolsRequest.builder().maxResults(50).build())
                .userPools().stream()
                .filter(p -> p.name().contains(cognitoUserPoolName))
                .findAny()
                .orElseThrow(() -> new RuntimeException("User pool %s not found".formatted(cognitoUserPoolName)))
                .id();

        logger.log("userPoolId: %s".formatted(userPoolId));
        return userPoolId;
    }

    private String getClientId() {
        String clientId = cognitoClient.listUserPoolClients(
                        ListUserPoolClientsRequest.builder().userPoolId(cognitoUserPoolId).maxResults(1).build())
                .userPoolClients().stream()
                .filter(c -> c.clientName().contains(CLIENT_APP))
                .findAny()
                .orElseThrow(() -> new RuntimeException("User pool %s not found".formatted(cognitoUserPoolName)))
                .clientId();

        logger.log("clientId: %s".formatted(clientId));
        return clientId;
    }

    private boolean isValidPassword(String password) {
        String pattern = "^(?=.*[0-9])(?=.*[A-Za-z])(?=.*[$%^*_@])(?=\\S+$).{12,}$";
        return password.matches(pattern);
    }

    private boolean isValidSignupRequest(UserRecord request) {
        return StringUtils.isNoneBlank(request.email) &&
                StringUtils.isNoneBlank(request.password) &&
                StringUtils.isNoneBlank(request.lastName);
    }

    /**
     * {
     * "firstName": // string
     * "lastName": // string
     * "email": // email validation
     * "password": // alphanumeric + any of "$%^*", 12+ chars
     * }
     */
    private record UserRecord(String email, String firstName, String lastName, String password) {
    }

    /**
     * {
     * "email": // email
     * "password": // alphanumeric + any of "$%^*", 12+ chars
     * }
     */
    private record SinginRecord(String email, String password) {
    }
}
