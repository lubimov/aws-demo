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
        System.out.println( openapi.getForecast());
    }
}
