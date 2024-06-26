package com.task11;

import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import software.amazon.awssdk.utils.CollectionUtils;

import java.util.Map;

public class AbstractRequestHandlers {
    protected static final int SC_OK = 200;
    protected static final int SC_BAD_REQUEST = 400;

    protected final Gson gson = new GsonBuilder().setPrettyPrinting().create();
    protected final Map<String, String> responseHeaders = Map.of("Content-Type", "application/json");

    protected APIGatewayProxyResponseEvent buildResponse(int statusCode, String jsonBody) {
        return new APIGatewayProxyResponseEvent()
                .withStatusCode(statusCode)
                .withHeaders(responseHeaders)
                .withBody(jsonBody);
    }

    protected APIGatewayProxyResponseEvent buildErrorResponse(String message) {
        Map<String, String> response = Map.of(
                "statusCode", String.valueOf(SC_BAD_REQUEST),
                "message", "ERROR. %s.".formatted(message)
        );

        return buildResponse(SC_BAD_REQUEST, gson.toJson(response));
    }

    protected APIGatewayProxyResponseEvent badResponse(APIGatewayProxyRequestEvent requestEvent) {
        Map<String, String> response = Map.of(
                "statusCode", String.valueOf(SC_BAD_REQUEST),
                "message", "Bad request syntax or unsupported method. Request path: %s. HTTP method: %s".formatted(
                        getPath(requestEvent), getMethod(requestEvent))
        );

        return buildResponse(SC_BAD_REQUEST, gson.toJson(response));
    }

    protected String getMethod(APIGatewayProxyRequestEvent requestEvent) {
        return requestEvent.getHttpMethod();
    }

    protected String getPath(APIGatewayProxyRequestEvent requestEvent) {
        String path = requestEvent.getPath();
        if (CollectionUtils.isNotEmpty(requestEvent.getPathParameters()) && path.contains("/tables/")) {
            path = "/tables_id";
        }
        return path;
    }

    protected record RouteKey(String method, String path) {
    }

}
