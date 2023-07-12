using System;
using System.Collections.Generic;

namespace Remitee.Services.Metrics.Models;

public partial class PartnersOperation
{
    public Guid Id { get; set; }

    public DateTime Date { get; set; }

    public string Type { get; set; } = null!;

    public string? Description { get; set; }

    public string SourceCurrency { get; set; } = null!;

    public string? TargetCurrency { get; set; }

    public string? Partner { get; set; } = null!;

    public decimal Amount { get; set; }

    public decimal? ExchangeRate { get; set; }

    public PartnersOperation()
    {

    }
    public PartnersOperation(DateTime date, string type, string description, string sourceCurrency, string targetCurrency, string partner, decimal amount, decimal? exchangeRate)
    {
        Id = Guid.NewGuid();
        Date = date;
        Type = type;
        Description = description;
        SourceCurrency = sourceCurrency;
        TargetCurrency = targetCurrency;
        Partner = partner;
        Amount = amount;
        ExchangeRate = exchangeRate;
    }

}
