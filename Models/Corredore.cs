using System;
using System.Collections.Generic;

namespace Remitee.Services.Metrics.Models;

public partial class Corredore
{
    public Guid Id { get; set; }

    public int Year { get; set; }

    public int Month { get; set; }

    public string SourceCountryName { get; set; } = null!;

    public string SourceCountryCode { get; set; } = null!;

    public string TargetCountryName { get; set; } = null!;

    public string TargetCountryCode { get; set; } = null!;

    public int Count { get; set; }

    public int CountRemitee { get; set; }

    public int CountInbound { get; set; }

    public int CountTopup { get; set; }

    public int CountMt { get; set; }

    public decimal GtvRemitee { get; set; }

    public decimal GtvInbound { get; set; }

    public decimal GtvTotal { get; set; }

    public decimal GtvTopup { get; set; }

    public decimal GtvMt { get; set; }

    public decimal NtvRemitee { get; set; }

    public decimal NtvInbound { get; set; }

    public decimal NtvTotal { get; set; }

    public decimal NtvTopup { get; set; }

    public decimal NtvMt { get; set; }

    public decimal? FeeRemitee { get; set; }

    public decimal? FeeInbound { get; set; }

    public decimal? FeeTotal { get; set; }

    public decimal? SpreadRemitee { get; set; }

    public decimal? SpreadInbound { get; set; }

    public decimal? SpreadTotal { get; set; }

    public Corredore()
    {

    }

    public Corredore(int year, int month, string sourceCountryName, string sourceCountryCode, string targetCountryName, string targetCountryCode, int count, int countRemitee, int countInbound, int countTopup, int countMt, decimal gtvRemitee, decimal gtvInbound, decimal gtvTotal, decimal gtvTopup, decimal gtvMt, decimal ntvRemitee, decimal ntvInbound, decimal ntvTotal, decimal ntvTopup, decimal ntvMt, decimal? feeRemitee, decimal? feeInbound, decimal? feeTotal, decimal? spreadRemitee, decimal? spreadInbound, decimal? spreadTotal)
    {
        Id = Guid.NewGuid();
        Year = year;
        Month = month;
        SourceCountryName = sourceCountryName;
        SourceCountryCode = sourceCountryCode;
        TargetCountryName = targetCountryName;
        TargetCountryCode = targetCountryCode;
        Count = count;
        CountRemitee = countRemitee;
        CountInbound = countInbound;
        CountTopup = countTopup;
        CountMt = countMt;
        GtvRemitee = gtvRemitee;
        GtvInbound = gtvInbound;
        GtvTotal = gtvTotal;
        GtvTopup = gtvTopup;
        GtvMt = gtvMt;
        NtvRemitee = ntvRemitee;
        NtvInbound = ntvInbound;
        NtvTotal = ntvTotal;
        NtvTopup = ntvTopup;
        NtvMt = ntvMt;
        FeeRemitee = feeRemitee;
        FeeInbound = feeInbound;
        FeeTotal = feeTotal;
        SpreadRemitee = spreadRemitee;
        SpreadInbound = spreadInbound;
        SpreadTotal = spreadTotal;
    }
}
