using System;
using System.Collections.Generic;

namespace Remitee.Services.Metrics.Models;

public partial class ComplexChurn
{
    public Guid Id { get; set; }

    public string Type { get; set; } = null!;

    public Guid EndPointId { get; set; }

    public string? SourceCountryName { get; set; }

    public string? SourceCountryCode { get; set; }

    public string? TargetCountryName { get; set; }

    public string? TargetCountryCode { get; set; }

    public int CurrentMonth { get; set; }

    public int CurrentYear { get; set; }

    public int FirstTrxMonth { get; set; }

    public int FirstTrxYear { get; set; }

    public string Partner { get; set; } = null!;

    public decimal? Gtv { get; set; }

    public decimal? GtvTopup { get; set; }

    public decimal? GtvMt { get; set; }

    public int Count { get; set; }

    public int CountTopup { get; set; }

    public int CountMt { get; set; }

    public decimal? GtvAvg { get; set; }

    public ComplexChurn()
    {

    }

    public ComplexChurn(string type, Guid endPointId, string? sourceCountryName, string? sourceCountryCode, string? targetCountryName, string? targetCountryCode, int currentMonth, int currentYear, int firstTrxMonth, int firstTrxYear, string partner, decimal? gtv, decimal? gtvTopup, decimal? gtvMt, int count, int countTopup, int countMt, decimal? gtvAvg)
    {
        Id = Guid.NewGuid();
        Type = type;
        EndPointId = endPointId;
        SourceCountryName = sourceCountryName;
        SourceCountryCode = sourceCountryCode;
        TargetCountryName = targetCountryName;
        TargetCountryCode = targetCountryCode;
        CurrentMonth = currentMonth;
        CurrentYear = currentYear;
        FirstTrxMonth = firstTrxMonth;
        FirstTrxYear = firstTrxYear;
        Partner = partner;
        Gtv = gtv;
        GtvTopup = gtvTopup;
        GtvMt = gtvMt;
        Count = count;
        CountTopup = countTopup;
        CountMt = countMt;
        GtvAvg = gtvAvg;
    }
}
