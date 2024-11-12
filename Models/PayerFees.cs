using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Remitee.Services.Metrics.Models
{
    public class PayerFees
    {
        public Guid FeeId { get; set; }

        public FeeType Type { get; set; }

        public string Currency { get; set; } = null!;

        public decimal? Min { get; set; }

        public decimal? Max { get; set; }

        public decimal? Percentage { get; set; }

        public decimal? Fix { get; set; }

        public decimal? Start { get; set; }

        public decimal? End { get; set; }

        public int? Priority { get; set; }

        public Guid FeeStrategy { get; set; }
    }

    public enum FeeType
    {
        General,
        QuantityScale,
        TransactionAmountScale
    }
}
