using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Remitee.Services.Metrics.Models
{
    public class CollectorDataDTO
    {
        public int? TransactionCollectorTransactionId { get; set; }

        public Guid SenderUniqueId { get; set; }

        public Guid ReceiverUniqueId { get; set; }

        public bool ProcessedInCollector { get; set; }

        public int? LedgerTransactionId { get; set; }
    }
}
