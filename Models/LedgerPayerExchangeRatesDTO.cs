using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Remitee.Services.Metrics.Models
{
    public class LedgerPayerExchangeRatesDTO
    {
        public int Id { get; set; }

        public DateTime CreatedDate { get; set; }

        public DateTime ValidFrom { get; set; }

        public DateTime ValidTo { get; set; }

        public decimal TargetToUSD { get; set; }

        public string PayerName { get; set; }

        public string PayerCurrency { get; set; }
    }
}
