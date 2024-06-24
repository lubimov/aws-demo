package com.task10;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;
import software.amazon.awssdk.services.cognitoidentityprovider.CognitoIdentityProviderClient;
import software.amazon.awssdk.services.cognitoidentityprovider.model.*;

import java.util.HashMap;
import java.util.Map;

public class AuthHandler extends AbstractRequestHandlers {
    private LambdaLogger logger;
    private String cognitoUserPoolName;

    private static final String CLIENT_APP = "client-app";

    CognitoIdentityProviderClient cognitoClient = CognitoIdentityProviderClient.create();


    public AuthHandler(Context context) {
        logger = context.getLogger();
        cognitoUserPoolName = System.getenv("cognito_userpool");
    }

    public APIGatewayV2HTTPResponse handleSignin(APIGatewayV2HTTPEvent requestEvent) {
        logger.log(">> handleSignin");
        final SinginRecord request = gson.fromJson(requestEvent.getBody(), SinginRecord.class);

        Map<String, String> authParameters = new HashMap<>();
        authParameters.put("USERNAME", request.email);
        authParameters.put("PASSWORD", request.password);

        InitiateAuthRequest authRequest = InitiateAuthRequest.builder()
                .authFlow(AuthFlowType.USER_PASSWORD_AUTH)
                .clientId(CLIENT_APP)
                .authParameters(authParameters)
                .build();

        InitiateAuthResponse authResponse = cognitoClient.initiateAuth(authRequest);
        String accessToken = authResponse.authenticationResult().idToken(); //.accessToken();

        Map<String, String> response = Map.of("accessToken", accessToken);
        return buildResponse(SC_OK, gson.toJson(response));
    }

    public APIGatewayV2HTTPResponse handleSignup(APIGatewayV2HTTPEvent requestEvent) {
        logger.log(">> handleSignup");
        final UserRecord request = gson.fromJson(requestEvent.getBody(), UserRecord.class);

        // Validate the password
        if (!isValidPassword(request.password)) {
            return buildErrorResponse("Invalid password. The password must be alphanumeric, " +
                    "include at least one of the special characters $%^*, and be at least 12 characters long.");
        }

        AdminCreateUserRequest createUserRequest = AdminCreateUserRequest.builder()
                .userPoolId(cognitoUserPoolName)
                .username(request.email)
                .userAttributes(
                        AttributeType.builder()
                                .name("email")
                                .value(request.email)
                                .build(),
                        AttributeType.builder()
                                .name("given_name")
                                .value(request.firstName)
                                .build(),
                        AttributeType.builder()
                                .name("family_name")
                                .value(request.lastName)
                                .build())
                .temporaryPassword("TempPassword123!")
                .messageAction(MessageActionType.SUPPRESS)
                .build();

        try {
            AdminCreateUserResponse createUserResponse = cognitoClient.adminCreateUser(createUserRequest);

            // Set user's permanent password
            AdminSetUserPasswordRequest setPasswordRequest = AdminSetUserPasswordRequest.builder()
                    .userPoolId(cognitoUserPoolName)
                    .username(request.email)
                    .password(request.password)
                    .permanent(true)
                    .build();
            cognitoClient.adminSetUserPassword(setPasswordRequest);

            return buildResponse(SC_OK, "");
        } catch (Exception e) {
            return buildErrorResponse(e.getMessage());
        }
    }

    private boolean isValidPassword(String password) {
        String pattern = "^(?=.*[0-9])(?=.*[a-z])(?=.*[A-Z])(?=.*[$%^*])(?=\\S+$).{12,}$";
        return password.matches(pattern);
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
