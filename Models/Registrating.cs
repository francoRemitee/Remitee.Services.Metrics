using System;
using System.Collections.Generic;

namespace Remitee.Services.Metrics.Models;

public partial class Registrating
{
    public Guid Id { get; set; }

    public string CountryName { get; set; } = null!;

    public int Year { get; set; }

    public int Month { get; set; }

    public int CompletedCount { get; set; }

    public string CountryCode { get; set; } = null!;
}
