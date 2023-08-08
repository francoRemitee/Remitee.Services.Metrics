using System;
using System.Collections.Generic;

namespace Remitee.Services.Metrics.Models;

public partial class FlatTransaction
{
    public int? TransactionCollectorTransactionId { get; set; }

    public Guid? MoneyTransferPaymentId { get; set; }

    public int? LedgerTransactionId { get; set; }

    public Guid? WalletOperationId { get; set; }

    public Guid? SenderWalletUserId { get; set; }

    public Guid? SenderWalletContactId { get; set; }

    public int? SenderLedgerPartyId { get; set; }

    public int? SenderLedgerUserId { get; set; }

    public Guid SenderUniqueId { get; set; }

    public string SenderType { get; set; } = null!;

    public string? SenderFirstName { get; set; }

    public string? SenderLastName { get; set; }

    public string? SenderCompleteName { get; set; }

    public string? SenderSendingCountry { get; set; }

    public string? SenderPostalCode { get; set; }

    public string? SenderAddressLine { get; set; }

    public string? SenderTown { get; set; }

    public string? SenderCountrySubdivision { get; set; }

    public string? SenderCountry { get; set; }

    public string? SenderEmail { get; set; }

    public string? SenderPhoneNumber { get; set; }

    public string? SenderDocumentNumber { get; set; }

    public string? SenderDocumentType { get; set; }

    public string? SenderDocumentIssuer { get; set; }

    public string? SenderNationality { get; set; }

    public string? SenderCountryOfBirth { get; set; }

    public DateTime? SenderDateOfBirth { get; set; }

    public string? SenderBankAccountNumber { get; set; }

    public string? SenderBankAccountBic { get; set; }

    public string? SenderBankAccountType { get; set; }

    public string? SenderTaxId { get; set; }

    public Guid? ReceiverWalletUserId { get; set; }

    public Guid? ReceiverWalletContactId { get; set; }

    public int? ReceiverLedgerUserId { get; set; }

    public int? ReceiverLedgerPartyId { get; set; }

    public Guid ReceiverUniqueId { get; set; }

    public string ReceiverType { get; set; } = null!;

    public string? ReceiverFirstName { get; set; }

    public string? ReceiverLastName { get; set; }

    public string? ReceiverCompleteName { get; set; }

    public string? ReceiverReceivingCountry { get; set; }

    public string? ReceiverPostalCode { get; set; }

    public string? ReceiverAddressLine { get; set; }

    public string? ReceiverTown { get; set; }

    public string? ReceiverCountrySubDivision { get; set; }

    public string? ReceiverCountry { get; set; }

    public string? ReceiverEmail { get; set; }

    public string? ReceiverPhoneNumber { get; set; }

    public string? ReceiverDocumentNumber { get; set; }

    public string? ReceiverDocumentType { get; set; }

    public string? ReceiverDocumentIssuer { get; set; }

    public string? ReceiverNationality { get; set; }

    public string? ReceiverCountryOfBirth { get; set; }

    public DateTime? ReceiverDateOfBirth { get; set; }

    public string? ReceiverBankAccountNumber { get; set; }

    public string? ReceiverBankAccountBic { get; set; }

    public string? ReceiverBankAccountType { get; set; }

    public string? ReceiverTaxId { get; set; }

    public bool ProcessedInCollector { get; set; }

    public string? Id { get; set; }

    public virtual Tctransaction? TransactionCollectorTransaction { get; set; }

    public virtual TransactionalBase? TransactionalBaseTransaction { get; set; }
}
