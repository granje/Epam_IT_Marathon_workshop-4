#! /bin/bash
cd DynamoDBApp
mvn clean package
cd target
mkdir -p java/lib
cp DynamoDBApp-1.0.jar java/lib/DynamoDBApp-1.0.jar
zip -r lambda-common-layer.zip java
cd classes
zip -r function-GetUserListFunction.zip handler/GetUserListFunction.class
zip -r function-GetUserListByQueryFunction.zip handler/GetUserListByQueryFunction.class
zip -r function-CreateUserFunction.zip handler/CreateUserFunction.class
zip -r function-DeleteUserFunction.zip handler/DeleteUserFunction.class
zip -r function-GetUserFunction.zip handler/GetUserFunction.class
zip -r function-UpdateUserFunction.zip handler/UpdateUserFunction.class
cd ../../..
mkdir lambda-deployment
mv DynamoDBApp/target/lambda-common-layer.zip lambda-deployment/lambda-common-layer.zip
mv DynamoDBApp/target/classes/function-GetUserListFunction.zip lambda-deployment/function-GetUserListFunction.zip
mv DynamoDBApp/target/classes/function-GetUserListByQueryFunction.zip lambda-deployment/function-GetUserListByQueryFunction.zip
mv DynamoDBApp/target/classes/function-CreateUserFunction.zip lambda-deployment/function-CreateUserFunction.zip
mv DynamoDBApp/target/classes/function-DeleteUserFunction.zip lambda-deployment/function-DeleteUserFunction.zip
mv DynamoDBApp/target/classes/function-GetUserFunction.zip lambda-deployment/function-GetUserFunction.zip
mv DynamoDBApp/target/classes/function-UpdateUserFunction.zip lambda-deployment/function-UpdateUserFunction.zip
