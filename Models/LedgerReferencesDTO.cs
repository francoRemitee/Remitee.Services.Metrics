using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Remitee.Services.Metrics.Models
{
    public class LedgerReferencesDTO
    {
        public int LedgerTransactionId { get; set; }

        public int? SenderLedgerUserId { get; set; }

        public int? SenderLedgerPartyId { get; set; }

        public int? ReceiverLedgerPartyId { get; set; }
    }
}
