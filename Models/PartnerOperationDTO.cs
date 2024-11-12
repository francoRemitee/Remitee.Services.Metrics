using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Remitee.Services.Metrics.Models
{
    public class PartnerOperationDTO
    {
        public string Id { get; set; }

        public DateTime CreatedAt { get; set; }

        public string? Type { get; set; } = null!;

        public string? Description { get; set; }

        public string SourceCurrency { get; set; } = null!;

        public string? TargetCurrency { get; set; }

        public decimal Amount { get; set; }

        public decimal? ExchangeRate { get; set; }

        public decimal? ExchangeRateOf { get; set; }
    }
}
