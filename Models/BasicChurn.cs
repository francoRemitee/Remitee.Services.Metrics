using System;
using System.Collections.Generic;

namespace Remitee.Services.Metrics.Models;

public partial class BasicChurn
{
    public Guid Id { get; set; }

    public int Year { get; set; }

    public int Month { get; set; }

    public int Churn { get; set; }

    public string CountryName { get; set; } = null!;

    public string CountryCode { get; set; } = null!;

    public int Count { get; set; }

    public BasicChurn()
    {

    }

    public BasicChurn(int year, int month, int churn, string countryName, string countryCode, int count)
    {
        Id = Guid.NewGuid();
        Year = year;
        Month = month;
        Churn = churn;
        CountryName = countryName;
        CountryCode = countryCode;
        Count = count;
    }
}
