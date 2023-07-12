using System;
using System.Collections.Generic;

namespace Remitee.Services.Metrics.Models;

public partial class ExchangeRate
{
    public Guid Id { get; set; }

    public string CountryName { get; set; } = null!;

    public string CountryCode { get; set; } = null!;

    public DateTime Date { get; set; }

    public decimal ExchangeRate1 { get; set; }

    public string OriginCurrency { get; set; } = null!;

    public string TargetCurrency { get; set; } = null!;

    public ExchangeRate()
    {

    }

    public ExchangeRate(string countryName, string countryCode, DateTime date, decimal exchangeRate1, string originCurrency, string targetCurrency)
    {
        Id = Guid.NewGuid();
        CountryName = countryName;
        CountryCode = countryCode;
        Date = date;
        ExchangeRate1 = exchangeRate1;
        OriginCurrency = originCurrency;
        TargetCurrency = targetCurrency;
    }
}
