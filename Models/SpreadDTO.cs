using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Remitee.Services.Metrics.Models
{
    public class SpreadDTO
    {
        public string Id { get; set; } = null!;

        public int? LedgerId { get; set; }

        public decimal? ReferenceExchangeRate { get; set; }

        public decimal? IbSpreadRate { get; set; }

        public decimal? ObSpreadRate { get; set; }

        public decimal? ObFeeAmount { get; set; }

        public string? ObFeeCurrency { get; set; }
    }
}
