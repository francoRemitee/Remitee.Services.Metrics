using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Remitee.Services.Metrics.Models
{
    public class FeeReferences
    {
        public Guid FeeStrategy { get; set; }

        public string PayerRouteCode { get; set; } = null!;

        public DateTime ValidFrom { get; set; }

        public DateTime ValidTo { get; set; }

        public ScopeType Scope { get; set; }
    }

    public enum ScopeType
    {
        Transactional,
        Global
    }
}
