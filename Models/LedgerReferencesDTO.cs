using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Remitee.Services.Metrics.Models
{
    public class LedgerReferencesDTO
    {
        public int Id { get; set; }

        public int? UserId { get; set; }

        public int? IBUserId { get; set; }

        public int? SenderPartyId { get; set; }

        public int? RecipientPartyId { get; set; }
    }
}
