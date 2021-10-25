using Amazon;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using Newtonsoft.Json;

// Pick one config
var config = GetLocalStackConfig();
// var config = GetAwsConfig();

// Program
var sqsClient = CreateSQSClient(config);
await CreateQueueAsync(config, sqsClient);
await SendMessageAsync(config, sqsClient);
await GetMessageAsync(config, sqsClient);
await DeleteQueueAsync(config, sqsClient);
Console.ReadKey();


static Config GetAwsConfig()
{
    return new Config(
        "00000",
        "https://sqs.us-east-1.amazonaws.com",
        "0000",
        "000000",
        "00000");
}

static Config GetLocalStackConfig()
{
    return new Config(
        "000000000000",
        "http://localhost:4566");
}

static AmazonSQSClient CreateSQSClient(Config config)
{
    return new AmazonSQSClient(
        config.SessionAWSCredentials,
        new AmazonSQSConfig
        {
            RegionEndpoint = RegionEndpoint.GetBySystemName(config.Region),
            ServiceURL = config.ServiceUrl
        });
}

static async Task CreateQueueAsync(Config config, AmazonSQSClient sqsClient)
{
    var createQueueRequest = new CreateQueueRequest
    {
        QueueName = config.QueueName
    };

    var response = await sqsClient.CreateQueueAsync(createQueueRequest);
    Console.WriteLine("Queue created");
    Console.WriteLine(JsonConvert.SerializeObject(response));
}

static async Task SendMessageAsync(Config config, AmazonSQSClient sqsClient)
{
    var sendMessageRequest = new SendMessageRequest
    {
        MessageBody =
            "{\"EventDate\":\"2021-10-25T08:48:58.3801711Z\",\"Id\":\"f603bb06-52d3-4dd7-b233-908c1e5d1b99\",\"Quantity\":1,\"MetaData\":null}",
        QueueUrl = config.QueueUrl,
        DelaySeconds = 0,
        MessageAttributes = new Dictionary<string, MessageAttributeValue>
        {
            {
                "version", new MessageAttributeValue
                {
                    DataType = "String",
                    StringValue = "2.0"
                }
            },
            {
                "content-type", new MessageAttributeValue
                {
                    DataType = "String",
                    StringValue = "application/json"
                }
            }
        }
    };

    var response = await sqsClient.SendMessageAsync(sendMessageRequest);
    Console.WriteLine("Message sent");
    Console.WriteLine(JsonConvert.SerializeObject(response));
}

static async Task GetMessageAsync(Config config, AmazonSQSClient sqsClient)
{
    var response = await sqsClient.ReceiveMessageAsync(new ReceiveMessageRequest
    {
        QueueUrl = config.QueueUrl,
        MaxNumberOfMessages = 10,
        WaitTimeSeconds = 20,

        // This line is the problem, program runs indefinitely with LocalStack
        // when commented out works fine but downloads messages without any MessageAttributes
        MessageAttributeNames = new List<string> {"All"}
    });

    Console.WriteLine("Message received");
    Console.WriteLine(JsonConvert.SerializeObject(response));
}

async Task DeleteQueueAsync(Config config, AmazonSQSClient sqsClient)
{
    var response = await sqsClient.DeleteQueueAsync(config.QueueUrl);

    Console.WriteLine("Queue deleted");
    Console.WriteLine(JsonConvert.SerializeObject(response));
}

public class Config
{
    public readonly string AccountId;
    public readonly string AwsAccessKeyId;
    public readonly string AwsSecretAccessKey;
    public readonly string QueueName;
    public readonly string Region;
    public readonly string ServiceUrl;
    public readonly string SessionToken;

    public Config(string accountId, string serviceUrl, string region = "us-east-1",
        string queueName = "local-stack-test-queue")
        : this(accountId, serviceUrl, "test", "test", "test", region, queueName)
    {
        AccountId = accountId;
        ServiceUrl = serviceUrl;
    }

    public Config(string accountId, string serviceUrl, string awsAccessKeyId, string awsSecretAccessKey,
        string sessionToken, string region = "us-east-1", string queueName = "spejson-test-queue")
    {
        AccountId = accountId;
        ServiceUrl = serviceUrl;
        AwsAccessKeyId = awsAccessKeyId;
        AwsSecretAccessKey = awsSecretAccessKey;
        SessionToken = sessionToken;
        Region = region;
        QueueName = queueName;
    }

    public string QueueUrl => $"{ServiceUrl}/{AccountId}/{QueueName}";

    public SessionAWSCredentials SessionAWSCredentials => new(AwsAccessKeyId, AwsSecretAccessKey, SessionToken);
}