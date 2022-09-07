using Confluent.Kafka;

// See https://aka.ms/new-console-template for more information
Console.WriteLine("Waiting for messages from Kafka!");

var kafkaConfig = new ConsumerConfig
{ 
    BootstrapServers = "localhost:9092",
    GroupId = "MyGroup",
    AutoOffsetReset = AutoOffsetReset.Earliest
};

CancellationTokenSource cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => {
    e.Cancel = true;
    cts.Cancel();
};

using (IConsumer<Ignore, string> consumer = new ConsumerBuilder<Ignore, string>(kafkaConfig).Build())
{
    consumer.Subscribe("MyTopic");
    try
    {
        while(true)
        {
            var response = consumer.Consume(cts.Token);
            if (response != null)
            {
                Console.WriteLine("Message from Kafka: " + response.Message.Value);
            }
            else
            {
                Console.WriteLine("Resposne is null");
            }
        }
    }
    catch(Exception ex)
    {
        Console.WriteLine(ex.Message);
    }
}
