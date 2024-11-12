using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Remitee.Services.Metrics.Models
{
    public class FxDTO
    {
        public string Id { get; set; }

        public decimal? NetAmountUsd { get; set; }

        public decimal? FeeAmountUsd { get; set; }

        public decimal? VatUsd { get; set; }

        public decimal? SpreadAmountUsd { get; set; }

        public decimal? SpreadAmountSc { get; set; }
    }
}
