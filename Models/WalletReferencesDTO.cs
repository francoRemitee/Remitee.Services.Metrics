using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Remitee.Services.Metrics.Models
{
    public class WalletReferencesDTO
    {
        public int? LedgerTransactionId { get; set; }

        public Guid SenderWalletUserId { get; set; }

        public Guid SenderWalletContactId { get; set; }

        public Guid ReceiverWalletContactId { get; set; }


    }
}
