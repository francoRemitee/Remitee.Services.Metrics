using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Remitee.Services.Metrics.Models
{
    public class PaymentQuoteElements
    {
        public Guid Id { get; set; }

        public int InternalTransactionId { get; set; }

        public decimal? PayerExchangeRate { get; set; }

        public decimal? PayerFee { get; set; }

        public decimal? PayerFeeExpected { get; set; }

        public decimal? PayerExchangeRateExpected { get; set; }

        public DateTime CreatedDate { get; set; }

        public decimal? RemiteeCalculatedFee { get; set; }
    }
}
