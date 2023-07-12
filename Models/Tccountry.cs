using System;
using System.Collections.Generic;
using System.Data;

namespace Remitee.Services.Metrics.Models;

public partial class Tccountry
{
    public string Id { get; set; } = null!;

    public string? Description { get; set; }

    public string? Isothree { get; set; }

    public string? Isotwo { get; set; }

    public Tccountry()
    {

    }

    public Tccountry(DataRow row)
    {
        Id = row.ItemArray[0].ToString();
        Description = row.ItemArray[1].ToString();
        Isothree = row.ItemArray[2].ToString();
        Isotwo = row.ItemArray[3].ToString();
    }
}
