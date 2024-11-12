using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Remitee.Services.Metrics.Models
{
    public class MarketExchangeRatesDTO
    {
        public string Id { get; set; }

        public decimal? ArsExchangeRate { get; set; }

        public decimal? ClpExchangeRate { get; set; }

        public decimal? ArsExchangeRateOf { get; set; }

        public decimal? MarketExchangeRate { get; set; }

    }
}
