using System;
using System.Collections.Generic;

namespace Remitee.Services.Metrics.Models;

public partial class Inbound
{
    public Guid Id { get; set; }

    public int Year { get; set; }

    public int Month { get; set; }

    public string Partner { get; set; } = null!;

    public string SourceCountryName { get; set; } = null!;

    public string SourceCountryCode { get; set; } = null!;

    public string TargetCountryName { get; set; } = null!;

    public string TargetCountryCode { get; set; } = null!;

    public int Count { get; set; }

    public int CountTopup { get; set; }

    public int CountMt { get; set; }

    public decimal GtvTotal { get; set; }

    public decimal GtvTopup { get; set; }

    public decimal GtvMt { get; set; }

    public decimal NtvTotal { get; set; }

    public decimal NtvTopup { get; set; }

    public decimal NtvMt { get; set; }

    public decimal? GtvAvg { get; set; }

    public decimal? Fee { get; set; }

    public decimal? Spread { get; set; }

    public decimal? Vat { get; set; }

    public Inbound()
    {

    }

    public Inbound(int year, int month, string partner, string sourceCountryName, string sourceCountryCode, string targetCountryName, string targetCountryCode, int count, int countTopup, int countMt, decimal gtvTotal, decimal gtvTopup, decimal gtvMt, decimal ntvTotal, decimal ntvTopup, decimal ntvMt, decimal? gtvAvg, decimal? fee, decimal? spread, decimal? vat)
    {
        Id = Guid.NewGuid();
        Year = year;
        Month = month;
        Partner = partner;
        SourceCountryName = sourceCountryName;
        SourceCountryCode = sourceCountryCode;
        TargetCountryName = targetCountryName;
        TargetCountryCode = targetCountryCode;
        Count = count;
        CountTopup = countTopup;
        CountMt = countMt;
        GtvTotal = gtvTotal;
        GtvTopup = gtvTopup;
        GtvMt = gtvMt;
        NtvTotal = ntvTotal;
        NtvTopup = ntvTopup;
        NtvMt = ntvMt;
        GtvAvg = gtvAvg;
        Fee = fee;
        Spread = spread;
        Vat = vat;
    }
}
