using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Remitee.Services.Metrics.Models
{
    public class TransactionalBaseCost
    {
        public string TransactionalBaseId { get; set; } = null!;

        public decimal? ObFeeAmount { get; set; }

        public decimal? ObFeeAmountUsd { get; set; }

        public decimal? ObFeeSpreadRate { get; set; }

        public decimal? ObSpreadAmountUsd { get; set; }

        public string? ObFeeCurrency { get; set; }
    }
}
