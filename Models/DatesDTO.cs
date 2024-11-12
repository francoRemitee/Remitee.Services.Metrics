using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Remitee.Services.Metrics.Models
{
    public class DatesDTO
    {
        public string Id { get; set; } = null!;

        public int? LedgerId { get; set; }

        public DateTime CreatedAt { get; set; }

        public DateTime? ForwardedAt { get; set; }

        public DateTime? SettledAt { get; set; }

        public DateTime? CompletedAt { get; set; }

        public DateTime? ReversedAt { get; set; }

        public string Status { get; set; }

        public DateTime? LastPushedAt { get; set; }
    }
}
