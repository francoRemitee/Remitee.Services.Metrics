using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Remitee.Services.Metrics.Models
{
    public class PayerFeeQuantityScaleDTO
    {
        public string Id { get; set; }

        public DateTime CreatedAt { get; set; }

        public int RowNumber { get; set; }

        public string? Currency { get; set; }

        public decimal? AmountUsd { get; set; }

        public decimal? AmountTc { get; set; }

        public decimal? ObExchangeRate { get; set; }


    }
}
