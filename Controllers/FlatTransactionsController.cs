using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using Remitee.Services.Metrics.Models;
using Remitee.Services.Metrics.ModelsTC;
using Remitee.Services.Metrics.Extensions;
using Microsoft.EntityFrameworkCore;

namespace Remitee.Services.Metrics.Controllers
{
    public class FlatTransactionsController
    {
        private readonly IConfiguration _configuration;
        public FlatTransactionsController(IConfiguration config)
        {
            _configuration = config;
        }

		public void UpdateData(DateTime dateFrom, DateTime dateTo)
        {
			var toUpdate = new List<FlatTransaction>();
			var updated = new List<FlatTransaction>();
			using (var ctx = new RemiteeServicesMetricsContext())
            {
				toUpdate = ctx.FlatTransactions.Where(x => x.ProcessedInCollector == false).ToList();
				toUpdate.AddCollectorDataExtension(dateFrom, dateTo, ctx);
				ctx.SaveChanges();
			}
	
        }

        public void UploadData(DateTime dateFrom, DateTime dateTo)
        {
			string ledgerQuery = @"select
cast(t.id as nvarchar(50)) as Id,
null as TransactionCollectorTransactionId,
null as MoneyTransferPaymentId,
t.[id] as LedgerTransactionId,
null as WalletOperationId,

null as SenderWalletUserId,
null as SenderWalletContactId,
sp.id as SenderLedgerPartyId,
u.id as SenderLedgerUserId,
newid() as SenderUniqueId,
'PERSON' as SenderType,
sp.firstName as SenderFirstName,
sp.lastName as SenderLastName,
concat(sp.firstName,' ',sp.lastName) as SenderCompleteName,
spc.isoCode as SenderSendingCountry,
sp.zipCode as SenderPostalCode,
sp.address as SenderAddressLine,
sp.city as SenderTown,
sp.state as SenderCountrySubdivision,
spc.isoCode as SenderCountry,
sp.email as SenderEmail,
sp.phoneNumber as SenderPhoneNumber,
sp.documentNumber as SenderDocumentNumber,
case sp.documentType
	when 'ARNU' then 'ARNU'
	when 'PASSPORT' then 'CCPT'
	when 'CUST' then 'CUST'
	when 'DRLC' then 'DRLC'
	when 'NATIONAL_ID' then 'NIDN'
	when 'TIN' then 'TXID'
	when 'SOS' then 'SOS'
	else 'UNKNOWN'
	end as SenderDocumentType,
sp.documentCountryCode as SenderDocumentIssuer,
null as SenderNationality,
sp.CountryOfBirth as SenderCountryOfBirth,
sp.birthDate as SenderDateOfBirth,
sp.bankAccountNumber as SenderBankAccountNumber,
spb.code as SenderBankAccountBIC,
case sp.bankAccountType
	when 1 then 'SVGS'
	when 2 then 'CACC'
	when 3 then 'CASH'
	else 'UNKNOWN'
	end as SenderBankAccountType,
sp.tin as SenderTaxId,

null as ReceiverWalletUserId,
null as ReceiverWalletContactId,
tp.id as ReceiverLedgerPartyId,
null as ReceiverLedgerUserId,
newid() as ReceiverUniqueId,
'PERSON' as ReceiverType,
tp.firstName as ReceiverFirstName,
tp.lastName as ReceiverLastName,
concat(tp.firstName,' ',tp.lastName) as ReceiverCompleteName,
tpc.isoCode as ReceiverReceivingCountry,
tp.zipCode as ReceiverPostalCode,
tp.address as ReceiverAddressLine,
tp.city as ReceiverTown,
tp.state as ReceiverCountrySubdivision,
tpc.isoCode as ReceiverCountry,
tp.email as ReceiverEmail,
tp.phoneNumber as ReceiverPhoneNumber,
tp.documentNumber as ReceiverDocumentNumber,
case tp.documentType
	when 'ARNU' then 'ARNU'
	when 'PASSPORT' then 'CCPT'
	when 'CUST' then 'CUST'
	when 'DRLC' then 'DRLC'
	when 'NATIONAL_ID' then 'NIDN'
	when 'TIN' then 'TXID'
	when 'SOS' then 'SOS'
	else 'UNKNOWN'
	end as ReceiverDocumentType,
tp.documentCountryCode as ReceiverDocumentIssuer,
null as ReceiverNationality,
tp.CountryOfBirth as ReceiverCountryOfBirth,
tp.birthDate as ReceiverDateOfBirth,
isnull(tp.bankAccountNumber,tpp.bankAccountNumber) as ReceiverBankAccountNumber,
tpb.code as ReceiverBankAccountBIC,
case tp.bankAccountType
	when 1 then 'SVGS'
	when 2 then 'CACC'
	when 3 then 'CASH'
	else 'UNKNOWN'
	end as ReceiverBankAccountType,
tp.tin as ReceiverTaxId


	

      
  FROM [dbo].[Transactions] t
  left join Parties sp on sp.id=t.senderPartyId
  left join Countries spc on spc.id=sp.countryId
  left join Banks spb on spb.id=sp.bankId
  left join Parties tp on tp.id=t.recipientPartyId
  left join Countries tpc on tpc.id=tp.countryId
  left join Banks tpb on tpb.id=tp.bankId
  left join PartyPaymentMethods tpp on tpp.id=t.recipientPartyCollectMethodId
  left join Users u on u.id=t.userId or u.id=t.IBuserId

  where t.sourceType=0 and t.createdDate>=@dateFrom and t.createdDate<@dateTo
and abs(t.status)>=3
";
			string moneyTransferQuery = @"select
cast(p.id as nvarchar(50)) as Id,
null as TransactionCollectorTransactionId,
p.Id as MoneyTransferPaymentId,
p.TransactionId as LedgerTransactionId,
null as WalletOperationId,

null as SenderWalletUserId,
null as SenderWalletContactId,
null as SenderLedgerPartyId,
null as SenderLedgerUserId,
newid() as SenderUniqueId,
case p.Type
when 1 then 'BUSSINESS'
when 0 then 'PERSON'
end as SenderType,
case p.Type
when 1 then isnull(p.UserInfo_Dbtr_Nm,p.UserInfo_Dbtr_StrdNm_FirstNm)
when 0 then p.UserInfo_Dbtr_StrdNm_FirstNm
end as SenderFirstName,
case p.Type
when 1 then isnull(p.UserInfo_Dbtr_Id_OrgId_Othr_SchmeNm_Prtry,p.UserInfo_Dbtr_StrdNm_LastNm)
when 0 then p.UserInfo_Dbtr_StrdNm_LastNm
end as SenderLastName,
concat(case p.Type
when 1 then isnull(p.UserInfo_Dbtr_Nm,p.UserInfo_Dbtr_StrdNm_FirstNm)
when 0 then p.UserInfo_Dbtr_StrdNm_FirstNm
end,' ',case p.Type
when 1 then isnull(p.UserInfo_Dbtr_Id_OrgId_Othr_SchmeNm_Prtry,p.UserInfo_Dbtr_StrdNm_LastNm)
when 0 then p.UserInfo_Dbtr_StrdNm_LastNm
end) as SenderCompleteName,
null as SenderSendingCountry,
p.UserInfo_Dbtr_PstlAdr_PstCd as SenderPostalCode,
p.UserInfo_Dbtr_PstlAdr_AdrLine as SenderAddressLine,
p.UserInfo_Dbtr_PstlAdr_TwnNm as SenderTown,
p.UserInfo_Dbtr_PstlAdr_CtrySubDvsn as SenderCountrySubdivision,
p.UserInfo_Dbtr_PstlAdr_Ctry as SenderCountry,
p.UserInfo_Dbtr_CtctDtls_EmailAdr as SenderEmail,
p.UserInfo_Dbtr_CtctDtls_PhneNb as SenderPhoneNumber,
case p.Type
when 1 then isnull(p.UserInfo_Dbtr_Id_OrgId_Othr_Id,p.UserInfo_Dbtr_Id_PrvId_Othr_Id)
when 0 then p.UserInfo_Dbtr_Id_PrvId_Othr_Id
end as SenderDocumentNumber,
case (case p.Type
		when 1 then isnull(p.UserInfo_Dbtr_Id_OrgId_Othr_SchmeNm_Cd,p.UserInfo_Dbtr_Id_PrvId_Othr_SchmeNm_Cd)
		when 0 then p.UserInfo_Dbtr_Id_PrvId_Othr_SchmeNm_Cd
		end)
	when 0 then 'ARNU'
	when 1 then 'CCPT'
	when 2 then 'CUST'
	when 3 then 'DRLC'
	when 4 then 'EMPL'
	when 5 then 'NIDN'
	when 6 then 'SOS'
	when 7 then 'TXID'
	else 'UNKNOWN' 
	end as senderDocumentType,
case p.Type
when 1 then isnull(p.UserInfo_Dbtr_Id_OrgId_Othr_Issr,p.UserInfo_Dbtr_Id_PrvId_Othr_Issr)
when 0 then p.UserInfo_Dbtr_Id_PrvId_Othr_Issr
end as SenderDocumentIssuer,
null as SenderNationality,
p.UserInfo_Dbtr_Id_PrvId_DtAndPlcOfBirth_CtryOfBirth as SenderCountryOfBirth,
try_convert(date,replace(p.UserInfo_Dbtr_Id_PrvId_DtAndPlcOfBirth_BirthDt,'-',''),112) as SenderDateOfBirth,
null as SenderBankAccountNumber,
null as SenderBankAccountBIC,
null as SenderBankAccountType,
case 
	when p.UserInfo_Dbtr_Id_PrvId_Othr_SchmeNm_Cd=7
	then p.UserInfo_Dbtr_Id_PrvId_Othr_Id
	when p.UserInfo_Dbtr_Id_OrgId_Othr_SchmeNm_Cd=7
	then p.UserInfo_Dbtr_Id_OrgId_Othr_Id
	else null
	end as SenderTaxId,

null as ReceiverWalletUserId,
null as ReceiverWalletContactId,
null as ReceiverLedgerPartyId,
null as ReceiverLedgerUserId,
newid() as ReceiverUniqueId,
case p.Type
when 2 then 'BUSSINESS'
else 'PERSON'
end as ReceiverType,
p.UserInfo_Cdtr_StrdNm_FirstNm as ReceiverFirstName,
p.UserInfo_Cdtr_StrdNm_LastNm as ReceiverLastName,
concat(p.UserInfo_Cdtr_StrdNm_FirstNm,' ',p.UserInfo_Cdtr_StrdNm_LastNm) as ReceiverCompleteName,
null as ReceiverReceivingCountry,
p.UserInfo_Cdtr_PstlAdr_PstCd as ReceiverPostalCode,
p.UserInfo_Cdtr_PstlAdr_AdrLine as ReceiverAddressLine,
p.UserInfo_Cdtr_PstlAdr_TwnNm as ReceiverTown,
p.UserInfo_Cdtr_PstlAdr_CtrySubDvsn as ReceiverCountrySubdivision,
p.UserInfo_Cdtr_PstlAdr_Ctry as ReceiverCountry,
p.UserInfo_Cdtr_CtctDtls_EmailAdr as ReceiverEmail,
p.UserInfo_Cdtr_CtctDtls_PhneNb as ReceiverPhoneNumber,
p.UserInfo_Cdtr_Id_PrvId_Othr_Id as ReceiverDocumentNumber,
case p.UserInfo_Cdtr_Id_PrvId_Othr_SchmeNm_Cd
	when 0 then 'ARNU'
	when 1 then 'CCPT'
	when 2 then 'CUST'
	when 3 then 'DRLC'
	when 4 then 'EMPL'
	when 5 then 'NIDN'
	when 6 then 'SOS'
	when 7 then 'TXID'
	else 'UNKNOWN' 
	end as ReceiverDocumentType,
p.UserInfo_Cdtr_Id_PrvId_Othr_Issr as ReceiverDocumentIssuer,
null as ReceiverNationality,
p.UserInfo_Cdtr_Id_PrvId_DtAndPlcOfBirth_CtryOfBirth as ReceiverCountryOfBirth,
try_convert(date,replace(p.UserInfo_Cdtr_Id_PrvId_DtAndPlcOfBirth_BirthDt,'-',''),112) as ReceiverDateOfBirth,
p.UserInfo_CdtrAcct_Id_Othr_Id as ReceiverBankAccountNumber,
p.UserInfo_CdtrAgt_FinInstnId_BIC as ReceiverBankAccountBIC,
case p.UserInfo_CdtrAcct_Type_Cd
	when 0 then 'SVGS'
	when 1 then 'CACC'
	when 2 then 'CASH'
	else 'UNKNOWN'
	end as ReceiverBankAccountType,
case p.UserInfo_Cdtr_Id_PrvId_Othr_SchmeNm_Cd
when 7 then p.UserInfo_Cdtr_Id_PrvId_Othr_Id
else null 
end as ReceiverTaxId
from mt.Payments p
where p.Status in (3,4,5) and p.CreatedAt>=@dateFrom and p.CreatedAt<@dateTo";
			string walletQuery = @"select 
cast(o.id as nvarchar(50)) as Id,
null as TransactionCollectorTransactionId,
null as MoneyTransferPaymentId,
ot.PaymentTransactionId as LedgerTransactionId,
o.id as WalletOperationId,

u.id as SenderWalletUserId,
uc.id as SenderWalletContactId,
null as SenderLedgerPartyId,
u.LedgerId as SenderLedgerUserId,
newid() as SenderUniqueId,
'PERSON' as SenderType,
uc.FirstName as SenderFirstName,
uc.LastName as SenderLastName,
u.Name as SenderCompleteName,
u.CountryId as SenderSendingCountry,
uc.Address_PostalCode as SenderPostalCode,
concat(uc.Address_Street,' ',uc.Address_Number) as SenderAddressLine,
uc.Address_City as SenderTown,
uc.Address_State as SenderCountrySubdivision,
uc.CountryOfResidenceId as SenderCountry,
u.Email as SenderEmail,
u.InternationalPhoneNumber as SenderPhoneNumber,
uc.Document_Number as SenderDocumentNumber,
case uc.Document_Type
	when 'ARNU' then 'ARNU'
	when 'PASSPORT' then 'CCPT'
	when 'DRLC' then 'DRLC'
	when 'NATIONAL_ID' then 'NIDN'
	else 'UNKNOWN' 
	end as senderDocumentType,
uc.Document_CountryId as SenderDocumentIssuer,
u.Nationality as SenderNationality,
null as SenderCountryOfBirth,
u.BirthDate as SenderDateOfBirth,
uc.BankAccount_Number as SenderBankAccountNumber,
(select value from string_split(uc.BankAccount_BankId,'-',1) where ordinal=2) as SenderBankAccountBIC,
case uc.BankAccount_Type
when 'SAVINGS' THEN 'SVGS'
when 'DEMAND_DEPOSIT' then 'CASH'
when 'CHECKING' then 'CACC'
else 'UNKNOWN'
end as SenderBankAccountType,
u.TaxId as SenderTaxId,

null as ReceiverWalletUserId,
c.id as ReceiverWalletContactId,
null as ReceiverLedgerPartyId,
null as ReceiverLedgerUserId,
newid() as ReceiverUniqueId,
'PERSON' as ReceiverType,
c.FirstName as ReceiverFirstName,
c.LastName as ReceiverLastName,
concat(c.FirstName,' ',c.LastName) as ReceiverCompleteName,
o.DestinationCountryId as ReceiverReceivingCountry,
c.Address_PostalCode as ReceiverPostalCode,
concat(c.Address_Street,' ',c.Address_Number) as ReceiverAddressLine,
c.Address_City as ReceiverTown,
c.Address_State as ReceiverCountrySubdivision,
c.CountryOfResidenceId as ReceiverCountry,
c.Email as ReceiverEmail,
c.InternationalPhoneNumber as ReceiverPhoneNumber,
c.Document_Number as ReceiverDocumentNumber,
case c.Document_Type
	when 'ARNU' then 'ARNU'
	when 'PASSPORT' then 'CCPT'
	when 'DRLC' then 'DRLC'
	when 'NATIONAL_ID' then 'NIDN'
	else 'UNKNOWN' 
	end as ReceiverDocumentType,
c.Document_CountryId as ReceiverDocumentIssuer,
null as ReceiverNationality,
null as ReceiverCountryOfBirth,
null as ReceiverDateOfBirth,
c.BankAccount_Number as ReceiverBankAccountNumber,
(select value from string_split(c.BankAccount_BankId,'-',1) where ordinal=2) as ReceiverBankAccountBIC,
(select value from string_split(uc.BankAccount_BankId,'-',1) where ordinal=2) as SenderBankAccountBIC,
case c.BankAccount_Type
when 'SAVINGS' THEN 'SVGS'
when 'DEMAND_DEPOSIT' then 'CASH'
when 'CHECKING' then 'CACC'
else 'UNKNOWN'
end as ReceiverBankAccountType,
c.BankAccount_TaxId as ReceiverTaxId
from wallet.Operations o
left join wallet.OperationTransactions ot on ot.OperationId=o.Id
left join wallet.ShoppingCarts sc on sc.id=o.ShoppingCartId
left join wallet.Users u on u.id=sc.UserId
left join wallet.Contacts uc on uc.id=u.ContactId
left join wallet.Contacts c on c.id=o.ContactId
where o.CreatedDateUTC>=@dateFrom and o.CreatedDateUTC<@dateTo
								and o.OperationType='TOPUPS'
								and o.State in ('COMPLETED','REVERSED')";
			var results = new List<FlatTransaction>();

			using (var conn = new SqlConnection(_configuration.GetConnectionString("MoneyTransferConnString")))
            {
				results.AddRange(conn.Query<FlatTransaction>(moneyTransferQuery,new {dateFrom = dateFrom, dateTo = dateTo}));
			}
			using (var conn = new SqlConnection(_configuration.GetConnectionString("LedgerConnString")))
			{
				results.AddRange(conn.Query<FlatTransaction>(ledgerQuery, new { dateFrom = dateFrom, dateTo = dateTo }));
			}
			using (var conn = new SqlConnection(_configuration.GetConnectionString("WalletConnString")))
			{
				results.AddRange(conn.Query<FlatTransaction>(walletQuery, new { dateFrom = dateFrom, dateTo = dateTo }));
			}
			results = AddCollectorData(results, dateFrom, dateTo);
			results = FixCountryCodes(results);
			results = AddUsersReferences(results, dateFrom, dateTo);

			using(var ctx = new RemiteeServicesMetricsContext())
            {
				foreach(var item in results)
                {
					var entry = ctx.FlatTransactions.Find(item.Id);

					if (entry == null)
					{
						ctx.FlatTransactions.Add(item);
					}
					else
					{
						ctx.Entry(entry).CurrentValues.SetValues(item);
					}
				}
				
				ctx.SaveChanges();
            }
		}

		private List<FlatTransaction> AddCollectorData(List<FlatTransaction> list, DateTime dateFrom, DateTime dateTo)
        {
			using (var ctx = new RemiteeServicesMetricsContext())
            {
				ctx.ChangeTracker.AutoDetectChangesEnabled = false;
				ctx.Database.SetCommandTimeout(300);
				var tcReferences = ctx.Tctransactions.Where(x => x.DateCreated >= dateFrom && x.DateCreated < dateTo).Select(x => new { Id = x.Id, ReferenceId = x.TrxReference, SenderId = x.SenderId, ReceiverId = x.ReceiverId }).ToList();
				foreach (var item in list)
				{
					item.ProcessedInCollector = false;
					if (tcReferences.Select(x => x.ReferenceId?.ToUpper()).Contains(item.LedgerTransactionId.ToString()))
                    {
						item.TransactionCollectorTransactionId = tcReferences.First(x => x.ReferenceId?.ToUpper() == item.LedgerTransactionId.ToString()?.ToUpper()).Id;
						item.SenderUniqueId = Guid.Parse(tcReferences.First(x => x.ReferenceId?.ToUpper() == item.LedgerTransactionId.ToString()?.ToUpper()).SenderId);
						item.ReceiverUniqueId = Guid.Parse(tcReferences.First(x => x.ReferenceId?.ToUpper() == item.LedgerTransactionId.ToString()?.ToUpper()).ReceiverId);
						item.ProcessedInCollector = true;
                    }
					else if(tcReferences.Select(x => x.ReferenceId?.ToUpper()).Contains(item.MoneyTransferPaymentId.ToString()))
					{
						item.TransactionCollectorTransactionId = tcReferences.First(x => x.ReferenceId?.ToUpper() == item.MoneyTransferPaymentId.ToString()?.ToUpper()).Id;
						item.SenderUniqueId = Guid.Parse(tcReferences.First(x => x.ReferenceId?.ToUpper() == item.LedgerTransactionId.ToString()?.ToUpper()).SenderId);
						item.ReceiverUniqueId = Guid.Parse(tcReferences.First(x => x.ReferenceId?.ToUpper() == item.LedgerTransactionId.ToString()?.ToUpper()).ReceiverId);
						item.ProcessedInCollector = true;
					}
				}
			}
			
			return list;
        }

		

		private List<FlatTransaction> FixCountryCodes(List<FlatTransaction> list)
        {
			var countries = new List<Tccountry>();
			using(var ctx = new RemiteeServicesMetricsContext())
            {
				countries = ctx.Tccountries.ToList();
            }
			var toFix = list.Where(x => x.SenderCountryOfBirth?.Length == 2).ToList();
			foreach(var item in list)
            {
				if(item.SenderCountryOfBirth?.Length == 2)
                {
					item.SenderCountryOfBirth = countries.First(x => x.Isotwo == item.SenderCountryOfBirth.ToUpper()).Isothree;
				}
				if (item.SenderDocumentIssuer?.Length == 2)
				{
					item.SenderDocumentIssuer = countries.First(x => x.Isotwo == item.SenderDocumentIssuer.ToUpper()).Isothree;
				}
				if (item.ReceiverDocumentIssuer?.Length == 2)
				{
					item.ReceiverDocumentIssuer = countries.First(x => x.Isotwo == item.ReceiverDocumentIssuer.ToUpper()).Isothree;
				}
				if (item.ReceiverCountryOfBirth?.Length == 2)
				{
					item.ReceiverCountryOfBirth = countries.First(x => x.Isotwo == item.ReceiverCountryOfBirth.ToUpper()).Isothree;
				}

			}
			return list;

		}

		private List<FlatTransaction> AddUsersReferences(List<FlatTransaction> list, DateTime dateFrom, DateTime dateTo)
        {
			var references = new List<LedgerReferencesDTO>();
			var referencesWallet = new List<WalletReferencesDTO>();
			using (var conn = new SqlConnection(_configuration.GetConnectionString("LedgerConnString")))
			{
				var query = @"select t.id,t.userId,t.ibuserId,t.senderPartyId,t.recipientPartyId from Transactions t where t.sourceType=0 and t.createdDate>=@dateFrom and t.createdDate<@dateTo
and abs(t.status)>=3 ";
                references.AddRange(conn.Query<LedgerReferencesDTO>(query, new { dateFrom = dateFrom, dateTo = dateTo }));
			}
			using (var conn = new SqlConnection(_configuration.GetConnectionString("WalletConnString")))
			{
				var query = @"select ot.PaymentTransactionId,
u.Id as SenderUserId,
uc.Id as SenderContactId,
c.Id as RecipientContactId
from wallet.Operations o
inner join wallet.OperationTransactions ot on ot.OperationId=o.Id
inner join wallet.ShoppingCarts sc on sc.id=o.ShoppingCartId
inner join wallet.Users u on u.id=sc.UserId
inner join wallet.Contacts uc on uc.id=u.ContactId
inner join wallet.Contacts c on c.id=o.ContactId
where o.CreatedDateUTC>=@dateFrom and o.CreatedDateUTC<@dateTo
								and o.OperationType='TOPUPS'
								and o.State in ('COMPLETED','REVERSED')";
				referencesWallet.AddRange(conn.Query<WalletReferencesDTO>(query, new { dateFrom = dateFrom, dateTo = dateTo }));
			}
			foreach (var item in list)
            {
				var ids = references.Select(x => x.Id).ToList();
				var walletIds = referencesWallet.Select(x => x.PaymentTransactionId).ToList();
				if(ids.Contains(item.LedgerTransactionId ?? 0))
                {
					var temp = references.First(x => x.Id == item.LedgerTransactionId);
					item.SenderLedgerUserId = temp.UserId ?? temp.IBUserId;
					item.SenderLedgerPartyId = temp.SenderPartyId;
					item.ReceiverLedgerPartyId = temp.RecipientPartyId;
                }
				else if (walletIds.Contains(item.LedgerTransactionId ?? 0))
				{
					var temp = referencesWallet.First(x => x.PaymentTransactionId == item.LedgerTransactionId);
					item.SenderWalletUserId = temp.SenderUserId;
					item.SenderWalletContactId = temp.SenderContactId;
					item.ReceiverWalletContactId = temp.RecipientContactId;
				}
			}
			return list;
		}

	}
}
