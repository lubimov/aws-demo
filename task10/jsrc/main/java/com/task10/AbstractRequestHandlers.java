package com.task10;

import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import software.amazon.awssdk.utils.CollectionUtils;
import software.amazon.awssdk.utils.MapUtils;

import java.util.Collections;
import java.util.Map;

public class AbstractRequestHandlers {
    protected static final int SC_OK = 200;
    protected static final int SC_BAD_REQUEST = 400;

    protected static final String ACCESS_TOKEN_HEADER = "accessToken";
    protected static final String AUTHORIZATIONS_HEADER = "Authorization";
    protected final Gson gson = new GsonBuilder().setPrettyPrinting().create();
    protected final Map<String, String> responseHeaders = Map.of("Content-Type", "application/json");

    protected APIGatewayV2HTTPResponse buildResponse(int statusCode, String jsonBody) {
        return APIGatewayV2HTTPResponse.builder()
                .withStatusCode(statusCode)
                .withHeaders(responseHeaders)
                .withBody(jsonBody)
                .build();
    }

    protected APIGatewayV2HTTPResponse buildErrorResponse(String message) {
        Map<String, String> response = Map.of(
                "message","ERROR. %s.".formatted(message)
        );

        return buildResponse(SC_BAD_REQUEST, gson.toJson(response));
    }

    protected APIGatewayV2HTTPResponse badResponse(APIGatewayV2HTTPEvent requestEvent) {
        Map<String, String> response = Map.of(
                "message","Bad request syntax or unsupported method. Request path: %s. HTTP method: %s".formatted(
                getPath(requestEvent), getMethod(requestEvent))
        );

        return buildResponse(SC_BAD_REQUEST, gson.toJson(response));
    }

    protected String getMethod(APIGatewayV2HTTPEvent requestEvent) {
        return requestEvent.getRequestContext().getHttp().getMethod();
    }

    protected String getPath(APIGatewayV2HTTPEvent requestEvent) {
        String path = requestEvent.getRequestContext().getHttp().getPath();
        if (CollectionUtils.isNotEmpty(requestEvent.getPathParameters())){
            path = path + "_id";
        }
        return path;
    }
    protected record RouteKey(String method, String path) {
    }

}
