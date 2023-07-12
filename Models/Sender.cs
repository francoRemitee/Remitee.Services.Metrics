using System;
using System.Collections.Generic;

namespace Remitee.Services.Metrics.Models;

public partial class Sender
{
    public Guid Id { get; set; }

    public int Year { get; set; }

    public int Month { get; set; }

    public string CountryName { get; set; } = null!;

    public string CountryCode { get; set; } = null!;

    public int Count { get; set; }

    public string Type { get; set; } = null!;

    public Sender()
    {

    }

    public Sender(int year, int month, string countryName, string countryCode, int count, string type)
    {
        Id = Guid.NewGuid();
        Year = year;
        Month = month;
        CountryName = countryName;
        CountryCode = countryCode;
        Count = count;
        Type = type;
    }
}
