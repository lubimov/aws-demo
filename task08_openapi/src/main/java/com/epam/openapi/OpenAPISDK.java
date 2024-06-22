package com.epam.openapi;

import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.Map;

public class OpenAPISDK {
    public String getForecast()  {
        try {
            // init connection
            URL url = new URL("https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&current=temperature_2m,wind_speed_10m&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m");
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("GET");

            // add parameters
            // Map<String, String> parameters = new HashMap<>();
            // parameters.put("param1", "val");

            // con.setDoOutput(true);
            // DataOutputStream out = new DataOutputStream(con.getOutputStream());
            // out.writeBytes(ParameterStringBuilder.getParamsString(parameters));
            // out.flush();
            // out.close();

            // set timeouts
            // con.setConnectTimeout(5000);
            // con.setReadTimeout(5000);

            // Execute request
            try {
                int status = con.getResponseCode();
                if (status < 299) {
                    try (BufferedReader in = new BufferedReader(
                            new InputStreamReader(con.getInputStream()))) {
                        String inputLine;
                        StringBuffer content = new StringBuffer();
                        while ((inputLine = in.readLine()) != null) {
                            content.append(inputLine);
                        }

                        return content.toString();
                    }
                }

                return "{" +
                        "\"statusCode\" : " + status + "," +
                        "\"message\" : \'" + con.getResponseMessage() + "\"" +
                        "}";
            } finally {
                con.disconnect();
            }

        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static class ParameterStringBuilder {
        public static String getParamsString(Map<String, String> params)
                throws UnsupportedEncodingException {
            StringBuilder result = new StringBuilder();

            for (Map.Entry<String, String> entry : params.entrySet()) {
                result.append(URLEncoder.encode(entry.getKey(), "UTF-8"));
                result.append("=");
                result.append(URLEncoder.encode(entry.getValue(), "UTF-8"));
                result.append("&");
            }

            String resultString = result.toString();
            return resultString.length() > 0
                    ? resultString.substring(0, resultString.length() - 1)
                    : resultString;
        }
    }
}
