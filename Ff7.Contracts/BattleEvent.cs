namespace Ff7.Contracts;

public record BattleEvent(
    Guid EventId,
    string BattleId,
    string CharacterName,
    string Action,
    string Target,
    DateTime OccurredAtUtc);
