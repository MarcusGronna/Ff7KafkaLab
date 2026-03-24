using Confluent.Kafka;
using Ff7.Contracts;
using Scalar.AspNetCore;
using System.Text.Json;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOpenApi();

builder.Services.AddSingleton<IProducer<string, string>>(_ =>
{
    var config = new ProducerConfig
    {
        BootstrapServers = "localhost:9092",
        ClientId = "ff7-producer-api",
        Acks = Acks.All,
        EnableIdempotence = true
    };

    return new ProducerBuilder<string, string>(config).Build();
});

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
    app.MapScalarApiReference();
}

app.MapPost("/battle-events", async (
    CreateBattleEventRequest request,
    IProducer<string, string> producer) =>
{
    var battleEvent = new BattleEvent(
        EventId: Guid.NewGuid(),
        BattleId: request.BattleId,
        CharacterName: request.CharacterName,
        Action: request.Action,
        Target: request.Target,
        OccurredAtUtc: DateTime.UtcNow);

    var json = JsonSerializer.Serialize(battleEvent);

    var result = await producer.ProduceAsync(
        "ff7.battle-events",
        new Message<string, string>
        {
            Key = battleEvent.BattleId,
            Value = json
        });

    return Results.Ok(new
    {
        message = "Battle event produced",
        battleEvent.EventId,
        battleEvent.BattleId,
        result.Topic,
        Partition = result.Partition.Value,
        Offset = result.Offset.Value
    });
})
.WithName("CreateEvent")
.WithSummary("Create an FF7 battle event and publishes it to Kafka.");

app.MapGet("/health", () => "Healthy");

app.Run();

public record CreateBattleEventRequest(
    string BattleId,
    string CharacterName,
    string Action,
    string Target);