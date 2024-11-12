using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Remitee.Services.Metrics.Models
{
    public class BillingDTO
    {
        public int LedgerId { get; set; }

        public string? BillingClientName { get; set; }

        public string? BillingClientDocumentNumber { get; set; }

        public int BillingSiiFolio { get; set; }

        public decimal BillingTotalAmount { get; set; }

        public decimal BillingNetTaxed { get; set; }

        public decimal BillingTaxAmount { get; set; }
    }
}
