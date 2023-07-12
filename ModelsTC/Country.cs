using System;
using System.Collections.Generic;

namespace Remitee.Services.Metrics.ModelsTC;

public partial class Country
{
    public string Id { get; set; } = null!;

    public string? Description { get; set; }

    public string? Isothree { get; set; }

    public string? Isotwo { get; set; }
}
