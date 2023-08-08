using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Remitee.Services.Metrics.Models
{
    public class WalletReferencesDTO
    {
        public int? PaymentTransactionId { get; set; }

        public Guid SenderUserId { get; set; }

        public Guid SenderContactId { get; set; }

        public Guid RecipientContactId { get; set; }


    }
}
