package com.epam;

import com.epam.openapi.OpenAPISDK;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        OpenAPISDK openapi = new OpenAPISDK();
        final String url = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&current=temperature_2m,wind_speed_10m&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m";
        System.out.println( openapi.getForecast(url));
    }
}
