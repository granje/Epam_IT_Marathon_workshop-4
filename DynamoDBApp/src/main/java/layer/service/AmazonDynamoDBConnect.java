package layer.service;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import lombok.Getter;

@Getter
public class AmazonDynamoDBConnect {

    private DynamoDBMapper dynamoDBMapper;

    public AmazonDynamoDBConnect() {

        final AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();

        this.dynamoDBMapper = new DynamoDBMapper(client);
    }
}
