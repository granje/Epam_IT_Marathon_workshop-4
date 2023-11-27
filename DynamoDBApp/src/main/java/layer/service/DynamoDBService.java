package layer.service;

import java.util.Map;

public interface DynamoDBService {

    String getUsersListResponse(Map<String, String> queryParameters);

    String getUsersListByQueryResponse(Map<String, String> queryParameters, String inputBody);

    String createUser(String inputBody);

    String updateUser(Map<String, String> pathParameters, String inputBody);

    String deleteUser(Map<String, String> pathParameters);

    String findUser(Map<String, String> pathParameters);
}
