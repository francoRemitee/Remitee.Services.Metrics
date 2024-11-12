using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Remitee.Services.Metrics.Models
{
    public partial class TbModifiedFields
    {
        public string TransactionalBaseId { get; set; } = null!;

        public decimal? NetAmountUsd { get; set; }

        public decimal? IbSpreadAmountSc { get; set; }

        public decimal? IbSpreadAmountUsd { get; set; }

        public decimal? IbFeeAmountUsd { get; set; }

        public decimal? IbVatUsd { get; set; }

        public decimal? ObSpreadAmountTc { get; set; }

        public decimal? ObSpreadAmountUsd { get; set; }

        public decimal? ObFeeAmountTc { get; set; }

        public decimal? ObFeeAmountUsd { get; set; }

        public string? ObFeeCurrency { get; set; }

        public decimal? ObExchangeRate { get; set; }

        public decimal? ObExchangeRateAccounting { get; set; }

        public decimal? ObMarketExchangeRate { get; set; }

        public decimal? Payed { get; set; }

    }
}
