using System;
using System.Collections.Generic;
using System.Data;

namespace Remitee.Services.Metrics.Models;

public partial class Tctransaction
{
    public int Id { get; set; }

    public string? SenderId { get; set; }

    public string? ReceiverId { get; set; }

    public decimal TrxAmount { get; set; }

    public string? TrxCurrencyCode { get; set; }

    public string? TrxType { get; set; }

    public DateTime TrxDate { get; set; }

    public string? TrxReference { get; set; }

    public string? SendingClerkOrBranchId { get; set; }

    public string? SendingInstitutionId { get; set; }

    public string? SendingIntitutionName { get; set; }

    public string? SendingInstitutionCountryCode { get; set; }

    public string? ReceivingInstitutionId { get; set; }

    public string? ReceivingInstitutionName { get; set; }

    public string? ReceivingInstitutionCountryCode { get; set; }

    public string? SenderName { get; set; }

    public string? SenderAddress { get; set; }

    public string? SenderMethod { get; set; }

    public string? ReceiverName { get; set; }

    public string? ReceiverAddress { get; set; }

    public string? ReceiverMethod { get; set; }

    public string? SenderAgentName { get; set; }

    public string? SenderAgentAddress { get; set; }

    public string? SenderAgentOccupation { get; set; }

    public string? SenderAgentId { get; set; }

    public string? ReceiverAgentName { get; set; }

    public string? ReceiverAgentAddress { get; set; }

    public string? ReceiverAgentId { get; set; }

    public string? ReceiverAgentOccupation { get; set; }

    public string? SenderSegment { get; set; }

    public string? ReceiverSegment { get; set; }

    public string? PaymentStatus { get; set; }

    public string? ExchangeRate { get; set; }

    public string? TrxFeesAndTaxes { get; set; }

    public string? TrxChannel { get; set; }

    public string? PromotionId { get; set; }

    public decimal SenderAmount { get; set; }

    public string? SenderCurrencyCode { get; set; }

    public decimal ReceiverAmount { get; set; }

    public string? ReceiverCurrencyCode { get; set; }

    public DateTime DateCreated { get; set; }

    public DateTime? DateCompleted { get; set; }

    public string? TrxDescription { get; set; }

    public bool IsRejected { get; set; }

    public DateTime? FileDateCreated { get; set; }

    public string? FileNameSaved { get; set; }

    public virtual ICollection<FlatTransaction> FlatTransactions { get; } = new List<FlatTransaction>();
}
