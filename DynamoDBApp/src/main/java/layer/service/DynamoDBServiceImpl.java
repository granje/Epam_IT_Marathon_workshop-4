package layer.service;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.QueryResultPage;
import com.amazonaws.services.dynamodbv2.datamodeling.ScanResultPage;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.gson.Gson;
import layer.model.RequestBody;
import layer.model.ResponseMessage;
import layer.model.User;

import java.text.MessageFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class DynamoDBServiceImpl implements DynamoDBService {

    public static final String TABLE_PARTITION_KEY = "email";
    public static final String INDEX_PARTITION_KEY = "country";
    public static final String INDEX_PARTITION_KEY_VALUE = "Ukraine";
    public static final String COUNTRY_NAME_INDEX = "country-name-index";
    public static final String COUNTRY_LOCATION_INDEX = "country-location-index";
    public static final String COUNTRY_BIRTHDAY_INDEX = "country-birthday-index";
    public static final String LIMIT_QUERY_PARAMETER = "limit";
    public static final String HASH_KEY_QUERY_PARAMETER = "hashkey";
    public static final String RANGE_KEY_QUERY_PARAMETER = "rangekey";
    public static final Set<String> SOCIAL_MEDIA_NAMES =
            Set.of("linkedin", "telegram", "skype", "instagram", "facebook");
    public static final String NAME_BODY_PARAMETER = "name";
    public static final String LOCATION_BODY_PARAMETER = "location";
    public static final String BIRTHDAY_BODY_PARAMETER = "birthday";
    public static final int MAX_AGE = 150;
    public static final int MIN_AGE = 0;

    /**
     * Instance of a mapper class that provides methods for performing create, read,
     * update, and delete (CRUD) operations on a DynamoDB table.
     * A mapper can also perform query and scan operations to retrieve data from the table.
     */
    private static final DynamoDBMapper dynamoDBMapper = new AmazonDynamoDBConnect().getDynamoDBMapper();

    @Override
    public String createUser(String inputBody) {

        User user = new Gson().fromJson(inputBody, User.class);
        user.setCountry(INDEX_PARTITION_KEY_VALUE);

        User existingUser = dynamoDBMapper.load(User.class, user.getEmail());
        if (existingUser == null) {
            if (isValidSocialMedia(user.getSocialMedia())) {

                dynamoDBMapper.save(user);
                return getJsonResponse("User created: " + user.getEmail());

            } else return getJsonResponse("User with such social media links cannot be created");

        } else return getJsonResponse("User with this email already exists");
    }

    @Override
    public String findUser(Map<String, String> pathParameters) {

        User user = new User();
        user.setEmail(pathParameters.get(TABLE_PARTITION_KEY));

        User existingUser = dynamoDBMapper.load(User.class, user.getEmail());
        if (existingUser != null) {
            return new Gson().toJson(existingUser);
        } else {
            return getJsonResponse("User not found");
        }
    }

    @Override
    public String updateUser(Map<String, String> pathParameters, String inputBody) {

        //TODO Implement a record update by Id
        /*
        Add code that accepts parameters, finds required record by Id,
        and update it in the table record according to provided parameters.
        Returns a text message as operation result.
        */
    }

    @Override
    public String deleteUser(Map<String, String> pathParameters) {

        User userToDelete = new User();
        userToDelete.setEmail(pathParameters.get(TABLE_PARTITION_KEY));

        User existingUser = dynamoDBMapper.load(User.class, userToDelete.getEmail());
        if (existingUser != null) {
            dynamoDBMapper.delete(existingUser);
            return getJsonResponse("User deleted: " + existingUser.getEmail());
        } else {
            return getJsonResponse("User not found");
        }
    }

    @Override
    public String getUsersListResponse(Map<String, String> queryParameters) {
        return getNotFilteredUsersList(queryParameters);
    }

    @Override
    public String getUsersListByQueryResponse(Map<String, String> queryParameters, String inputBody) {
        if (inputBody != null) {
            RequestBody bodyParameters = extractRequestBodyParameters(inputBody);
            if (bodyParameters != null) {
                if (isValidNameParameter(bodyParameters)) {
                    return getFilteredUsersList(queryParameters, COUNTRY_NAME_INDEX, NAME_BODY_PARAMETER,
                            bodyParameters.getName());
                } else if (isValidLocationParameter(bodyParameters)) {
                    return getFilteredUsersList(queryParameters, COUNTRY_LOCATION_INDEX, LOCATION_BODY_PARAMETER,
                            bodyParameters.getLocation());
                } else if (isValidAgeParameter(bodyParameters)) {
                    return getFilteredUsersList(queryParameters, BIRTHDAY_BODY_PARAMETER,
                            bodyParameters.getAgeLimits().get(0), bodyParameters.getAgeLimits().get(1),
                            COUNTRY_BIRTHDAY_INDEX);
                }
            }
        }
        return getNotFilteredUsersList(queryParameters);
    }

    private static void updateUsersNotNullAttributes(User existingUser, User inputUser) {
        if (inputUser.getCountry() != null) {
            existingUser.setLocation(inputUser.getCountry());
        }
        if (inputUser.getName() != null) {
            existingUser.setName(inputUser.getName());
        }
        if (inputUser.getLocation() != null) {
            existingUser.setLocation(inputUser.getLocation());
        }
        if (inputUser.getBirthday() != null) {
            existingUser.setBirthday(inputUser.getBirthday());
        }
        if (inputUser.getRegistration() != null) {
            existingUser.setRegistration(inputUser.getRegistration());
        }
        if (inputUser.getAvatar() != null) {
            existingUser.setAvatar(inputUser.getAvatar());
        }
        if (inputUser.getAbout() != null) {
            existingUser.setAbout(inputUser.getAbout());
        }
        if (inputUser.getInterests() != null) {
            existingUser.setInterests(inputUser.getInterests());
        }
        if (inputUser.getSocialMedia() != null) {
            existingUser.setSocialMedia(inputUser.getSocialMedia());
        }
        if (inputUser.getPrivacy() != null) {
            existingUser.setPrivacy(inputUser.getPrivacy());
        }
    }

    private RequestBody extractRequestBodyParameters(String inputBody) {
        try {
            return new Gson().fromJson(inputBody, RequestBody.class);
        } catch (Exception e) {
            return null;
        }
    }

    private String getNotFilteredUsersList(Map<String, String> queryParameters) {
        if (hasValidLimit(queryParameters)) {
            return getPaginatedNotFilteredUsersList(queryParameters);
        } else {
            return getNotPaginatedNotFilteredUsersList();
        }
    }

    private String getFilteredUsersList(Map<String, String> queryParameters, String indexName,
                                        String queryParameter, String parameterValue) {
        if (hasValidLimit(queryParameters)) {
            return getPaginatedFilteredUsersList(queryParameters,
                    indexName, queryParameter, parameterValue);
        } else {
            return getNotPaginatedFilteredUsersList(indexName, queryParameter, parameterValue);
        }
    }

    private String getFilteredUsersList(Map<String, String> queryParameters, String queryParameter,
                                        String parameterLowValue, String parameterUpValue, String indexName) {
        if (hasValidLimit(queryParameters)) {
            return getPaginatedFilteredUsersList(queryParameters, indexName, queryParameter,
                    parameterLowValue, parameterUpValue);
        } else {
            return getNotPaginatedFilteredUsersList(indexName, queryParameter, parameterLowValue, parameterUpValue);
        }
    }

    private String getNotPaginatedNotFilteredUsersList() {
        List<User> users = dynamoDBMapper.scan(User.class, new DynamoDBScanExpression());
        return new Gson().toJson(users);
    }

    private String getPaginatedNotFilteredUsersList(Map<String, String> stringParameters) {

        String lastKey = extractHashKey(stringParameters);

        HashMap<String, AttributeValue> startKey = getTableStartKeyMap(lastKey);

        DynamoDBScanExpression scanExpression = new DynamoDBScanExpression()
                .withConsistentRead(false)
                .withLimit(getIntegerValue(extractLimit(stringParameters)))
                .withExclusiveStartKey(startKey);

        ScanResultPage<User> scanResultPage = dynamoDBMapper.scanPage(User.class, scanExpression);

        List<User> users = scanResultPage.getResults();

        return new Gson().toJson(users);
    }

    private String getNotPaginatedFilteredUsersList(String indexName, String sortKeyName, String sortKeyValue) {

        String partitionKeyAlias = "partAlias";
        String sortKeyAlias = "sortAlias";

        String partitionKeyLabel = "#" + INDEX_PARTITION_KEY;
        String sortKeyLabel = "#" + sortKeyName;

        HashMap<String, AttributeValue> expressionAttributeValues = getExpressionAttributeValuesMap(
                sortKeyValue, partitionKeyAlias, sortKeyAlias);

        String conditionExpression = MessageFormat.format(
                "{0} = :{1} and begins_with ({2}, :{3})",
                partitionKeyLabel, partitionKeyAlias,
                sortKeyLabel, sortKeyAlias);

        Map<String, String> expressionAttributeNames = getExpressionAttributeNamesMap(
                sortKeyName, partitionKeyLabel, sortKeyLabel);

        DynamoDBQueryExpression<User> queryExpression = new DynamoDBQueryExpression<User>()
                .withIndexName(indexName)
                .withConsistentRead(false)
                .withKeyConditionExpression(conditionExpression)
                .withExpressionAttributeNames(expressionAttributeNames)
                .withExpressionAttributeValues(expressionAttributeValues);

        QueryResultPage<User> queryResult = dynamoDBMapper.queryPage(User.class, queryExpression);
        List<User> users = queryResult.getResults();

        return new Gson().toJson(users);
    }

    private String getNotPaginatedFilteredUsersList(String indexName, String sortKeyName,
                                                    String sortKeyLowValue, String sortKeyUpValue) {

        Calendar cal = Calendar.getInstance();
        long currentTime = cal.getTimeInMillis() / 1000;
        long yearValue = getYearTimestampValue();

        String sortKeyLowValueStr = String.valueOf(currentTime - getOptionalIntegerValue(sortKeyUpValue)
                .orElse(MAX_AGE) * yearValue);
        String sortKeyUpValueStr = String.valueOf(currentTime - getOptionalIntegerValue(sortKeyLowValue)
                .orElse(MIN_AGE) * yearValue);

        String partitionKeyAlias = "partAlias";
        String sortKeyLowAlias = "sortLowAlias";
        String sortKeyUpAlias = "sortUpAlias";

        String partitionKeyLabel = "#" + INDEX_PARTITION_KEY;
        String sortKeyLabel = "#" + sortKeyName;

        HashMap<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":" + partitionKeyAlias,
                new AttributeValue().withS(INDEX_PARTITION_KEY_VALUE));
        expressionAttributeValues.put(":" + sortKeyLowAlias,
                new AttributeValue().withN(sortKeyLowValueStr));
        expressionAttributeValues.put(":" + sortKeyUpAlias,
                new AttributeValue().withN(sortKeyUpValueStr));

        String conditionExpression = MessageFormat.format(
                "{0} = :{1} and {2} between :{3} and :{4}",
                partitionKeyLabel, partitionKeyAlias,
                sortKeyLabel, sortKeyLowAlias, sortKeyUpAlias);

        Map<String, String> expressionAttributeNames = getExpressionAttributeNamesMap(
                sortKeyName, partitionKeyLabel, sortKeyLabel);

        DynamoDBQueryExpression<User> queryExpression = new DynamoDBQueryExpression<User>()
                .withIndexName(indexName)
                .withConsistentRead(false)
                .withKeyConditionExpression(conditionExpression)
                .withExpressionAttributeNames(expressionAttributeNames)
                .withExpressionAttributeValues(expressionAttributeValues);

        QueryResultPage<User> queryResult = dynamoDBMapper.queryPage(User.class, queryExpression);
        List<User> users = queryResult.getResults();

        return new Gson().toJson(users);
    }

    private String getPaginatedFilteredUsersList(Map<String, String> stringParameters, String indexName,
                                                 String sortKeyName, String sortKeyValue) {

        HashMap<String, AttributeValue> startKey = getIndexStringStartKeyMap(sortKeyName,
                extractHashKey(stringParameters),
                extractRangeKey(stringParameters));

        String partitionKeyAlias = "partAlias";
        String sortKeyAlias = "sortAlias";

        String partitionKeyLabel = "#" + INDEX_PARTITION_KEY;
        String sortKeyLabel = "#" + sortKeyName;

        HashMap<String, AttributeValue> expressionAttributeValues = getExpressionAttributeValuesMap(
                sortKeyValue, partitionKeyAlias, sortKeyAlias);

        String conditionExpression = MessageFormat.format(
                "{0} = :{1} and begins_with ({2}, :{3})",
                partitionKeyLabel, partitionKeyAlias,
                sortKeyLabel, sortKeyAlias);

        Map<String, String> expressionAttributeNames = getExpressionAttributeNamesMap(
                sortKeyName, partitionKeyLabel, sortKeyLabel);

        DynamoDBQueryExpression<User> queryExpression = new DynamoDBQueryExpression<User>()
                .withIndexName(indexName)
                .withConsistentRead(false)
                .withKeyConditionExpression(conditionExpression)
                .withExpressionAttributeNames(expressionAttributeNames)
                .withExpressionAttributeValues(expressionAttributeValues)
                .withLimit(getIntegerValue(extractLimit(stringParameters)))
                .withExclusiveStartKey(startKey);

        QueryResultPage<User> queryResult = dynamoDBMapper.queryPage(User.class, queryExpression);
        List<User> users = queryResult.getResults();

        return new Gson().toJson(users);
    }

    private String getPaginatedFilteredUsersList(Map<String, String> stringParameters, String indexName,
                                                 String sortKeyName, String sortKeyLowValue, String sortKeyUpValue) {

        Calendar cal = Calendar.getInstance();
        long currentTime = cal.getTimeInMillis() / 1000;
        long yearValue = getYearTimestampValue();

        String sortKeyLowValueStr = String.valueOf(currentTime - getOptionalIntegerValue(sortKeyUpValue)
                .orElse(MAX_AGE) * yearValue);
        String sortKeyUpValueStr = String.valueOf(currentTime - getOptionalIntegerValue(sortKeyLowValue)
                .orElse(MIN_AGE) * yearValue);

        HashMap<String, AttributeValue> startKey = getIndexNumericStartKeyMap(sortKeyName,
                extractHashKey(stringParameters), extractRangeKey(stringParameters));

        String partitionKeyAlias = "partAlias";
        String sortKeyLowAlias = "sortLowAlias";
        String sortKeyUpAlias = "sortUpAlias";

        String partitionKeyLabel = "#" + INDEX_PARTITION_KEY;
        String sortKeyLabel = "#" + sortKeyName;

        HashMap<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":" + partitionKeyAlias,
                new AttributeValue().withS(INDEX_PARTITION_KEY_VALUE));
        expressionAttributeValues.put(":" + sortKeyLowAlias,
                new AttributeValue().withN(sortKeyLowValueStr));
        expressionAttributeValues.put(":" + sortKeyUpAlias,
                new AttributeValue().withN(sortKeyUpValueStr));

        String conditionExpression = MessageFormat.format(
                "{0} = :{1} and {2} between :{3} and :{4}",
                partitionKeyLabel, partitionKeyAlias,
                sortKeyLabel, sortKeyLowAlias, sortKeyUpAlias);

        Map<String, String> expressionAttributeNames = getExpressionAttributeNamesMap(
                sortKeyName, partitionKeyLabel, sortKeyLabel);

        DynamoDBQueryExpression<User> queryExpression = new DynamoDBQueryExpression<User>()
                .withIndexName(indexName)
                .withConsistentRead(false)
                .withKeyConditionExpression(conditionExpression)
                .withExpressionAttributeNames(expressionAttributeNames)
                .withExpressionAttributeValues(expressionAttributeValues)
                .withLimit(getIntegerValue(extractLimit(stringParameters)))
                .withExclusiveStartKey(startKey);

        QueryResultPage<User> queryResult = dynamoDBMapper.queryPage(User.class, queryExpression);
        List<User> users = queryResult.getResults();

        return new Gson().toJson(users);
    }

    private static HashMap<String, AttributeValue> getTableStartKeyMap(String lastHashKey) {
        HashMap<String, AttributeValue> startKey = new HashMap<>();
        if (isValidTableLastHashKey(lastHashKey)) {
            startKey.put(TABLE_PARTITION_KEY, new AttributeValue().withS(lastHashKey));
        } else {
            startKey = null;
        }
        return startKey;
    }

    private static HashMap<String, AttributeValue> getIndexStringStartKeyMap(String sortKeyName,
                                                                             String lastHashKey,
                                                                             String lastRangeKey) {
        HashMap<String, AttributeValue> startKey = new HashMap<>();
        if (isValidIndexHashAndRangeKeys(lastHashKey, lastRangeKey)) {
            startKey.put(TABLE_PARTITION_KEY, new AttributeValue().withS(lastHashKey));
            startKey.put(INDEX_PARTITION_KEY, new AttributeValue().withS(INDEX_PARTITION_KEY_VALUE));
            startKey.put(sortKeyName, new AttributeValue().withS(lastRangeKey));
        } else {
            startKey = null;
        }
        return startKey;
    }

    private static HashMap<String, AttributeValue> getIndexNumericStartKeyMap(String sortKeyName,
                                                                              String lastHashKey,
                                                                              String lastRangeKey) {
        HashMap<String, AttributeValue> startKey = new HashMap<>();
        if (isValidIndexHashAndRangeKeys(lastHashKey, lastRangeKey)) {
            startKey.put(TABLE_PARTITION_KEY, new AttributeValue().withS(lastHashKey));
            startKey.put(INDEX_PARTITION_KEY, new AttributeValue().withS(INDEX_PARTITION_KEY_VALUE));
            startKey.put(sortKeyName, new AttributeValue().withN(lastRangeKey));
        } else {
            return null;
        }
        return startKey;
    }

    private static Map<String, String> getExpressionAttributeNamesMap(String sortKeyName,
                                                                      String partitionKeyLabel,
                                                                      String sortKeyLabel) {
        Map<String, String> expressionAttributeNames = new HashMap<>();
        expressionAttributeNames.put(partitionKeyLabel, INDEX_PARTITION_KEY);
        expressionAttributeNames.put(sortKeyLabel, sortKeyName);
        return expressionAttributeNames;
    }

    private static HashMap<String, AttributeValue> getExpressionAttributeValuesMap(String sortKeyValue,
                                                                                   String partitionKeyAlias,
                                                                                   String sortKeyAlias) {
        HashMap<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":" + partitionKeyAlias,
                new AttributeValue().withS(INDEX_PARTITION_KEY_VALUE));
        expressionAttributeValues.put(":" + sortKeyAlias,
                new AttributeValue().withS(sortKeyValue));
        return expressionAttributeValues;
    }


    private static String extractLimit(Map<String, String> queryParameters) {
        return queryParameters.getOrDefault(LIMIT_QUERY_PARAMETER, null);
    }

    private static String extractHashKey(Map<String, String> queryParameters) {
        return queryParameters.getOrDefault(HASH_KEY_QUERY_PARAMETER, null);
    }

    private static String extractRangeKey(Map<String, String> queryParameters) {
        return queryParameters.getOrDefault(RANGE_KEY_QUERY_PARAMETER, null);
    }

    private boolean isValidSocialMedia(HashMap<String, String> socialMedia) {
        for (Map.Entry<String, String> entry : socialMedia.entrySet()) {
            if (!SOCIAL_MEDIA_NAMES.contains(entry.getKey())) {
                return false;
            }
        }
        return true;
    }

    private static boolean isValidAgeParameter(RequestBody bodyParameters) {
        return bodyParameters.getAgeLimits() != null && isValidLowerUpperAgeLimits(bodyParameters);
    }

    private static boolean isValidLowerUpperAgeLimits(RequestBody bodyParameters) {
        if (bodyParameters.getAgeLimits().get(0) != null && bodyParameters.getAgeLimits().get(1) != null) {
            Integer lowerLimit = getIntegerValue(bodyParameters.getAgeLimits().get(0));
            Integer upperLimit = getIntegerValue(bodyParameters.getAgeLimits().get(1));
            return lowerLimit != null && upperLimit != null &&
                    lowerLimit >= 0 && lowerLimit < upperLimit;
        } else return false;
    }

    private static boolean isValidLocationParameter(RequestBody bodyParameters) {
        return bodyParameters.getLocation() != null && !bodyParameters.getLocation().equals("");
    }

    private static boolean isValidNameParameter(RequestBody bodyParameters) {
        return bodyParameters.getName() != null && !bodyParameters.getName().equals("");
    }

    private static boolean isValidTableLastHashKey(String lastHashKey) {
        return lastHashKey != null && !lastHashKey.equals("");
    }

    private static boolean isValidIndexHashAndRangeKeys(String lastHashKey, String lastRangeKey) {
        return lastHashKey != null && lastRangeKey != null &&
                !lastHashKey.equals("") && !lastRangeKey.equals("");
    }

    private static boolean isValidLimit(Integer limit) {
        return limit != null && limit > 0;
    }

    private boolean hasValidLimit(Map<String, String> stringParameters) {
        if (stringParameters != null) {
            String limitStr = extractLimit(stringParameters);
            return limitStr != null && isValidLimit(getIntegerValue(limitStr));
        } else return false;
    }

    private static Integer getIntegerValue(String value) {
        try {
            return Integer.valueOf(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Optional<Integer> getOptionalIntegerValue(String value) {
        try {
            return Optional.of(Integer.valueOf(value));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    private long getYearTimestampValue() {
        Calendar cal = Calendar.getInstance();
        long currentTimestamp = cal.getTimeInMillis() / 1000;
        cal.add(Calendar.YEAR, 1);
        long nextYearTimestamp = cal.getTimeInMillis() / 1000;
        return nextYearTimestamp - currentTimestamp;
    }

    private static String getJsonResponse(String message) {
        return new Gson().toJson(ResponseMessage.builder()
                .message(message).build());
    }
}
