package layer.service;

import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;

public interface APIGatewayService {

    APIGatewayProxyResponseEvent getApiGatewayProxyResponseEvent(String output, int statusCode1);

}
