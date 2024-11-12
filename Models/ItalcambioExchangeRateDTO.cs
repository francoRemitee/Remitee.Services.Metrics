using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Remitee.Services.Metrics.Models
{
    public class ItalcambioExchangeRateDTO
    {
        public int LedgerId { get; set; }

        public DateTime CreatedAt { get; set; }

        public int RowNumber { get; set; }

        public decimal? ExchangeRate { get; set; }
    }
}
