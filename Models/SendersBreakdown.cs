using System;
using System.Collections.Generic;

namespace Remitee.Services.Metrics.Models;

public partial class SendersBreakdown
{
    public Guid Id { get; set; }

    public int Year { get; set; }

    public int Month { get; set; }

    public string CountryName { get; set; } = null!;

    public string CountryCode { get; set; } = null!;

    public int OnlyMtcount { get; set; }

    public int OnlyTopupcount { get; set; }

    public int Bothcount { get; set; }

    public string Type { get; set; } = null!;

    public SendersBreakdown()
    {

    }

    public SendersBreakdown(int year, int month, string countryName, string countryCode, int onlyMtcount, int onlyTopupcount, int bothcount, string type)
    {
        Id = Guid.NewGuid();
        Year = year;
        Month = month;
        CountryName = countryName;
        CountryCode = countryCode;
        OnlyMtcount = onlyMtcount;
        OnlyTopupcount = onlyTopupcount;
        Bothcount = bothcount;
        Type = type;
    }
}
