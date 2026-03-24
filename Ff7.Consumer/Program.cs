using Confluent.Kafka;
using Ff7.Contracts;
using System.Text.Json;

var config = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "ff7-battle-logger",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false
};

using var consumer = new ConsumerBuilder<string, string>(config)
    .SetErrorHandler((_, error) =>
    {
        Console.WriteLine($"Kafka error: {error.Reason}");
    })
    .Build();

consumer.Subscribe("ff7.battle-events");

Console.WriteLine("FF7 consumer is running. Waiting for battle events...");

try
{
    while (true)
    {
        var result = consumer.Consume(CancellationToken.None);

        var battleEvent = JsonSerializer.Deserialize<BattleEvent>(result.Message.Value);

        if (battleEvent is null)
        {
            Console.WriteLine("Skipping invalid payload.");
            continue;
        }

        Console.WriteLine("-----------------------------------------------------");
        Console.WriteLine($"Topic: {result.Topic}");
        Console.WriteLine($"Partition: {result.Partition.Value}");
        Console.WriteLine($"Offset: {result.Offset.Value}");
        Console.WriteLine($"Key (BattleId): {result.Message.Key}");
        Console.WriteLine(
            $"{battleEvent.CharacterName} uses {battleEvent.Action} on {battleEvent.Target} " +
            $"in battle {battleEvent.BattleId} at {battleEvent.OccurredAtUtc:O}");

        consumer.Commit(result);
    }
}
catch (OperationCanceledException)
{
}
finally
{
    consumer.Close();
}