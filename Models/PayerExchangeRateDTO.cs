using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Remitee.Services.Metrics.Models
{
    public class PayerExchangeRateDTO
    {
        public string Id { get; set; }

        [Column(TypeName = "decimal(20,8)")]
        public decimal? PayerExchangeRate { get; set; }
    }
}
