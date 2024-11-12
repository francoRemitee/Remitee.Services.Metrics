using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Remitee.Services.Metrics.Models
{
    public class ArsRExchangeRateDTO
    {
        public string Id { get; set; }

        public decimal? ArsrexchangeRate { get; set; }

        public decimal? ArsExchangeRateOf { get; set; }
    }
}
