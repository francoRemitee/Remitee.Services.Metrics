using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Remitee.Services.Metrics.Models
{
    public class ObFeeDTO
    {
        public string TransactionalBaseId { get; set; }

        public decimal? ObFeeAmountTc { get; set; }

        public string ObFeeCurrency { get; set; }

        public decimal? ObFeeAmountUsd { get; set; }
    }
}
