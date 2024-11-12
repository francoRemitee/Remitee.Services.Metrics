using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Remitee.Services.Metrics.Models
{
    public class PayerReference
    {
        public int LedgerId { get; set; }

        public string PayerReferenceId { get; set; }
    }
}
