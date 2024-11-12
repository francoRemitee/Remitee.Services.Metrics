using Microsoft.EntityFrameworkCore;
using Remitee.Services.Metrics.Controllers;
using Remitee.Services.Metrics.Extensions;
using Remitee.Services.Metrics.Models;
using System;
using System.Collections.Generic;
using System.Data;
using Microsoft.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using AutoMapper;
using Remitee.Services.Metrics.ModelsTC;
using System.Text.Json;
using Newtonsoft.Json;
using System.Globalization;
using System.ComponentModel;
using System.Net.Mail;
using System.Net;
using System.Configuration;
using Microsoft.Extensions.Configuration;
using Dapper;
using ClosedXML.Excel;
using Remitee.Services.Metrics.Extensions;

namespace Remitee.Services.Metrics.Services
{
    public class ImportService
    {
        private readonly IConfigurationRoot _configuration;
        public ImportService(IConfigurationRoot configuration)
        {
            _configuration = configuration;
        }

		public void UpdateTransactionalBase(DateTime dateFrom, DateTime dateTo)
		{
			var finalData = new List<TransactionalBase>();
			var parsedData = new List<TransactionalBase>();

			var dataWallet = new List<TransactionalBaseDTO>();
			var dataMoneyTransfer = new List<TransactionalBaseDTO>();

			string walletConnString = _configuration.GetConnectionString("WalletConnString");
			string moneyTransferConnString = _configuration.GetConnectionString("MoneyTransferConnString");

			var queryMoneyTransfer = @"select cast(p.Id as nvarchar(50)) as Id,
		p.transactionId as LedgerId,
		'MoneyTransfer' as Source,
		p.CreatedAt,
		case
			when c.Code='PAYRETAILERS'
			then 'ESP'
			when c.Code = 'PREXCARD'
			then 'URY'
			else isnull(p.BillingInfo_Ctry,p.UserInfo_Dbtr_PstlAdr_Ctry) 
			end as SourceCountryCode,
		q.TargetCountry as TargetCountryCode,
		case p.Status
			when 5 then 'REVERSED'
			when 4 then 'COMPLETED'
			when 3 then 'SETTLED'
			end as Status,
		case pa.CollectMethod
			when 0 then 'CASH'
			when 1 then 'MONEY TRANSFER'
			when 2 then 'WALLET'
			end as CollectMethod,
		c.Code as Client,
		concat(py.VendorCode,'-',q.CollectMethod, '-',q.TargetCurrency,'-',q.TargetCountry) as OBPartner,
		pa.Name as OBPartnerName,
		isnull(p.BillingInfo_Fx_Currency,'USD') as SourceCurrency,
		q.TargetCurrency,
		isnull(p.BillingInfo_NetAmount,qe.SendingAmount-qe.SendingFee-isnull(qe.SellerVATRemitee+qe.SellerVATMarketPlace,0)) as NetAmountSC,
		qe.SendingAmount-qe.sendingFee-isnull(qe.SellerVATRemitee+qe.SellerVATMarketPlace,0) as NetAmountUSD,
		isnull(p.BillingInfo_NetAmount+p.BillingInfo_Commission_Amount+p.BillingInfo_Vat_Amount,qe.SendingAmount) as GrossAmountSC,
		isnull(p.BillingInfo_Fx_Rate,qe.SourceToUSD) as ExchangeRateSC,
		isnull(p.BillingInfo_Fx_Spread,qe.FxSpread) as SpreadRate,
		case
		when qe.SellerFx>0
		then round((greatest(qe.RemiteeBaseFee,qe.RemiteePercentFee)+greatest(qe.MarketPlaceBaseFee,qe.MarketPlacePercentFee))/nullif(qe.sendingAmount,0),4)*qe.SellerFx*qe.SendingAmount/(1-isnull(qe.SellerFxSpread,0))*greatest(qe.RemiteeBaseFee,qe.RemiteePercentFee)/nullif(isnull(nullif(greatest(qe.RemiteeBaseFee,qe.RemiteePercentFee)+greatest(qe.MarketPlaceBaseFee,qe.MarketPlacePercentFee),0),qe.sendingFee),0)
		else isnull(p.BillingInfo_Commission_Amount,qe.sendingFee)
		end as FeeAmountSC,
		case 
		when c.Code='SANTANDERCHL' 
		then case
			when pa.collectMethod in (1,2) 
			then case
				when q.targetCountry in ('ARG','COL','MEX','PRY','VEN','BRA','PER','URY','BOL') then 1
				when q.targetCountry in ('ECU') then 1.75
				when q.targetCurrency = 'EUR' then 2
				when q.targetCountry in ('USA','GBR') then 2
				when q.targetCountry = 'HTI' then 4.5
				else 0
				end
			else case
				when q.targetCountry in ('ARG','COL','PRY','BRA','PER','BOL') then 2
				when q.targetCountry in ('ECU') then 1.75
				when q.targetCountry = 'VEN' then 1.5+(qe.SendingAmount-qe.sendingFee)*0.04
				else 0
				end
			end
		when c.Code='BANCO_ESTADO' 
		then case
			when pa.collectMethod in (1,2) 
			then case
				when q.targetCountry in ('ARG','BRA','MEX') then 1
				when q.targetCountry in ('ECU','BOL','PER','PRY','VEN','COL','URY','SEN') then 1.5
				when q.targetCurrency = 'EUR' then 2
				when q.targetCountry in ('USA','GTM','NIC','SLV','DOM','CRI') then 2
				when q.targetCountry = 'HTI' then 4.5
				when q.targetCountry = 'CHN' then 5
				else 0
				end
			else case
				when q.targetCountry in ('ARG','ECU','BOL','PRY','SEN') then 1.5
				when q.targetCountry in ('COL','PER') then 2
				when q.targetCountry = 'MEX' then 2.5
				when q.targetCountry in ('HTI','VEN') then 5
				else 0
				end
			end
		else isnull(nullif(greatest(qe.RemiteeBaseFee,qe.RemiteePercentFee),0),qe.sendingFee) 
		end as FeeAmountUSD,
		isnull(p.BillingInfo_Commission_Rate,qe.SendingFee/(qe.SendingAmount-qe.SendingFee)) as FeeRate,
		case
		when qe.SellerFx>0
		then round((greatest(qe.RemiteeBaseFee,qe.RemiteePercentFee)+greatest(qe.MarketPlaceBaseFee,qe.MarketPlacePercentFee))/nullif(qe.sendingAmount,0),4)*qe.SellerFx*qe.SendingAmount*qe.SellerVatPercent/(1-isnull(qe.SellerFxSpread,0))*greatest(qe.RemiteeBaseFee,qe.RemiteePercentFee)/nullif(isnull(nullif(greatest(qe.RemiteeBaseFee,qe.RemiteePercentFee)+greatest(qe.MarketPlaceBaseFee,qe.MarketPlacePercentFee),0),qe.sendingFee),0)
		else isnull(p.BillingInfo_Vat_Amount,qe.SellerVatRemitee)
		end as VATSC,
		qe.SellerVATRemitee as VATUSD,
		isnull(p.BillingInfo_Vat_Rate,qe.SellerVATRemitee/nullif(greatest(qe.RemiteeBaseFee,qe.RemiteePercentFee),0)) as VATRate,
		qe.ReceivingAmount as TargetAmountTC,
		qe.FxRate as ExchangeRateTC,
		qe.MarketExchangeRate,
		case 
			when py.vendorCode='Radar' and p.UserInfo_CdtrAgt_FinInstnId_BIC='BECHCLRM' then concat(py.Code,'-BE')
			when py.vendorCode='Radar' and p.UserInfo_CdtrAgt_FinInstnId_BIC!='BECHCLRM' then concat(py.Code,'-OTR')
			when py.Code='VE-ITALCAMBIO-VES-BANKACCOUNT' and qe.SendingAmount-qe.sendingFee-isnull(qe.SellerVATRemitee+qe.SellerVATMarketPlace,0)<100 then concat('VE-ITALCAMBIO-VES-BANKACCOUNT','-','SML')
			when py.Code='VE-ITALCAMBIO-VES-BANKACCOUNT' and qe.SendingAmount-qe.sendingFee-isnull(qe.SellerVATRemitee+qe.SellerVATMarketPlace,0)>=100 then concat('VE-ITALCAMBIO-VES-BANKACCOUNT','-','BIG')
			else py.Code
		end as PayerRoute,
		py.VendorCode as Payer,
		qe.ReceivingAmount*(1-isnull(p.WithholdingPercentages_IncomeWithholdingPercentage,0)-isnull(p.WithholdingPercentages_VatWithholdingPercentage,0)) as TargetAmountTCwithoutWithholding,
		p.WithholdingPercentages_IncomeWithholdingPercentage*qe.ReceivingAmount as WithholdingIncomeAmount,
		p.WithholdingPercentages_VatWithholdingPercentage*qe.ReceivingAmount as WithholdingVATAmount,
		p.WithholdingPercentages_IncomeWithholdingPercentage as WithholdingIncomeRate,
		p.WithholdingPercentages_VatWithholdingPercentage as WithholdingVATRate,
		qe.ReceivingAmount/nullif(isnull(p.BillingInfo_NetAmount,qe.SendingAmount-qe.SendingFee-isnull(qe.SellerVATRemitee+qe.SellerVATMarketPlace,0)),0) as 'AccountingFxRate',
		qe.ReceivingAmount/(nullif(isnull(p.BillingInfo_NetAmount,qe.SendingAmount-qe.SendingFee-isnull(qe.SellerVATRemitee+qe.SellerVATMarketPlace,0)),0)*(1-qe.FxSpread)) as 'AccountingFxRateWithoutSp',
		(isnull(p.BillingInfo_NetAmount,qe.SendingAmount-qe.SendingFee-isnull(qe.SellerVATRemitee+qe.SellerVATMarketPlace,0))*(1-qe.FxSpread)) as 'AccountingNetAmount',
		isnull(p.BillingInfo_NetAmount,qe.SendingAmount-qe.SendingFee-isnull(qe.SellerVATRemitee+qe.SellerVATMarketPlace,0))*isnull(p.BillingInfo_Fx_Spread,qe.FxSpread) as 'SpreadAmountSc',
		p.SettledAt,
		p.CompletedAt,
		p.ReversedAt,
		case
			when py.vendorCode = 'cienxcienbanco' then 'CIEN X CIEN BANCO'
			when py.code like '%movii%' then 'MOVII'
			when py.vendorCode = 'transferzero' and c.code='SANTANDERCHL' then 'TRANSFERZERO EUR'
			when py.vendorCode = 'transferzero' and c.code!='SANTANDERCHL' then 'TRANSFERZERO USD'
			when py.vendorCode = 'localpayment' and py.code like '%BRL%' then 'LOCAL PAY. Brazil'
			when py.vendorCode = 'localpayment' and py.code like '%MXN%' then 'LOCAL PAY. Mexico'
			when py.vendorCode = 'localpayment' and py.code like '%UYU%' then 'LOCAL PAY. Uruguay'
			when py.vendorCode = 'localpayment' and py.code like '%COP%' then 'LOCAL PAY. Colombia'
			when py.vendorCode = 'interbank' and py.code like '%PEN%' then 'INTERBANK Soles'
			when py.vendorCode = 'interbank' and py.code like '%USD%' then 'INTERBANK USD'
			when py.vendorCode = 'EASYPAGOS' then 'EASY PAGOS'
			when py.vendorCode = 'bancobisa' then 'BANCO BISA'
			when py.vendorCode = 'bancosol' then 'BANCO SOLIDARIO'
			when py.vendorCode = 'maxicambios' then 'MAXICAMBIO Outbound'
			when py.code like '%sogebank%' then 'SOGEBANK'
			when py.code like '%b89%' then 'B89'
			when py.vendorCode = 'pontual' then 'PONTUAL Outbound'
			else py.vendorCode
		end as Vendor,
		p.ReferenceId as ClientReferenceId,
		case 
		when qe.MarketPlaceBaseFee>0
		then round((greatest(qe.MarketPlaceBaseFee,qe.MarketPlacePercentFee)+greatest(qe.RemiteeBaseFee,qe.RemiteePercentFee))/nullif(qe.sendingAmount,0),4)*qe.SellerFx*qe.SendingAmount/(1-isnull(qe.SellerFxSpread,0))*greatest(qe.MarketPlaceBaseFee,qe.MarketPlacePercentFee)/nullif(isnull(nullif(greatest(qe.RemiteeBaseFee,qe.RemiteePercentFee)+greatest(qe.MarketPlaceBaseFee,qe.MarketPlacePercentFee),0),qe.sendingFee),0)
		else 0
		end as MarketPlaceFeeAmount,
		case 
		when qe.MarketPlaceBaseFee>0
		then round((greatest(qe.MarketPlaceBaseFee,qe.MarketPlacePercentFee)+greatest(qe.RemiteeBaseFee,qe.RemiteePercentFee))/nullif(qe.sendingAmount,0),4)*qe.SendingAmount/((1-isnull(qe.SellerFxSpread,0))*nullif(qe.amountUSD,0))*greatest(qe.MarketPlaceBaseFee,qe.MarketPlacePercentFee)/nullif(isnull(nullif(greatest(qe.RemiteeBaseFee,qe.RemiteePercentFee)+greatest(qe.MarketPlaceBaseFee,qe.MarketPlacePercentFee),0),qe.sendingFee),0)
		else 0
		end as MarketPlaceFeeRate,
		case 
		when qe.MarketPlaceBaseFee>0
		then round((greatest(qe.MarketPlaceBaseFee,qe.MarketPlacePercentFee)+greatest(qe.RemiteeBaseFee,qe.RemiteePercentFee))/nullif(qe.sendingAmount,0),4)*qe.SellerFx*qe.SendingAmount*qe.SellerVatPercent/(1-isnull(qe.SellerFxSpread,0))*greatest(qe.MarketPlaceBaseFee,qe.MarketPlacePercentFee)/nullif(isnull(nullif(greatest(qe.RemiteeBaseFee,qe.RemiteePercentFee)+greatest(qe.MarketPlaceBaseFee,qe.MarketPlacePercentFee),0),qe.sendingFee),0)
		else 0
		end as MarketPlaceVATAmount,
		qe.SellerVatPercent as MarketPlaceVATRate,
		p.payerPaymentCode as PayerReferenceId,
		qe.SellerFx/(1-isnull(qe.SellerFxSpread,0)) as MarketPlaceExchangeRate,
		greatest(qe.MarketPlaceBaseFee,qe.MarketPlacePercentFee) as MarketPlaceFeeAmountUsd,
		qe.SellerVATMarketPlace as MarketPlaceVatAmountUsd,
		p.ForwardedAt,
		o.Date as LastPushedAt,
		case 
			when c.Code='SANTANDERCHL' and q.TargetCurrency='EUR' then 2
			when c.Code!='REMITEE' then case
							when q.TargetCurrency='ARS' then 3
							when q.TargetCurrency='CLP' then 4
							else 1
							end
			when py.vendorCode in ('DTONE','Thunes') or py.vendorCode is null then case
							when isnull(p.BillingInfo_Ctry,p.UserInfo_Dbtr_PstlAdr_Ctry)='ARG' and q.TargetCountry='ARG' then 20
							when isnull(p.BillingInfo_Ctry,p.UserInfo_Dbtr_PstlAdr_Ctry)='ARG' then 21
							when isnull(p.BillingInfo_Ctry,p.UserInfo_Dbtr_PstlAdr_Ctry)='CHL' and q.TargetCountry='CHL' then 22
							else 23
							end
			when c.Code='REMITEE' then case
							when isnull(p.BillingInfo_Ctry,p.UserInfo_Dbtr_PstlAdr_Ctry)='CHL' then case
										when q.TargetCountry='ARG' then 10
										when q.TargetCountry='CHL' then 12
										else 15
										end
							when isnull(p.BillingInfo_Ctry,p.UserInfo_Dbtr_PstlAdr_Ctry)='ARG' then case
										when q.TargetCountry='ARG' then 11
										when q.TargetCountry='CHL' then 13
										else 14
										end
							end
			else -1
		end as TransactionType

								from mt.Payments p (NOLOCK)
								left join mt.QuoteElements qe (NOLOCK) on qe.QuoteId=p.QuoteId
								left join mt.Quotes q (NOLOCK) on q.id=qe.QuoteId
								left join mt.Clients c on c.Id=q.ClientId
								left join mt.PayerRoutes py on py.Id=p.payerRouteId
								left join mt.Payers pa on pa.id=q.PayerId
								left join mt.Agreements a on a.ClientId=p.ClientId and a.PayerId=q.PayerId
								LEFT JOIN (SELECT *, row_number() OVER (PARTITION BY paymentId ORDER BY Date DESC) as rn from mt.Operations x (NOLOCK)) o on o.PaymentId = p.id and o.rn=1
								where p.Status in (3,4,5) and p.CreatedAt>=@dateFrom and p.CreatedAt<@dateTo
								--and qe.AmountUSD>0
                                ";
			var queryWallet = @"select cast(o.Id as nvarchar(50)) as Id,
								null as LedgerId,
								'Wallet' as Source,
								o.CreatedDateUTC as CreatedAt,
								sc.Name as SourceCountryName,
								sc.Id as SourceCountryCode,
								tc.Name as TargetCountryName,
								tc.id as TargetCountryCode,
								o.State as Status,
								case o.OperationType
								when 'TOPUPS' then 'TOPUP'
								end as CollectMethod,
								'REMITEE' as Client,
								pc.Id as OBPartner,
								pm.Name as OBPartnerName,
								o.SourceCurrency as SourceCurrency,
								tc.CurrencyCode as TargetCurrency,
								o.SourceAmount as NetAmountSC,
								null as NetAmountUSD,
								case
									when o.SourceCurrency='CLP'
									then o.SourceAmount*1.1
									when o.SourceCurrency='ARS'
									then o.SourceAmount*1.13
									else 0
									end as GrossAmountSC,
								ps.ExchangeRate as ExchangeRateSC,
								ps.ExchangeRateSpread as SpreadRate,
								ps.CommissionRate*ps.NetAmmount as FeeAmountSC,
								ps.CommissionRate*ps.NetAmmount*ps.ExchangeRate as FeeAmountUSD,
								ps.CommissionRate as FeeRate,
								ps.VatRate*ps.CommissionRate*ps.NetAmmount as VATSC,
								ps.VatRate*ps.CommissionRate*ps.NetAmmount*ps.ExchangeRate as VATUSD,
								ps.VatRate as VATRate,
								o.DestinationAmount as TargetAmountTC,
								null as ExchangeRateTC,
								null as MarketExchangeRate,
								pc.code as PayerRoute,
								pc.Name as Payer,
								o.DestinationAmount as TargetAmountTCwithoutWithholding,
								null as WithholdingIncomeAmount,
								null as WothholdingVATAmount,
								null as WithholdingIncomeRate,
								null as WithholdingVATRate,
								o.DestinationAmount/o.SourceAmount as 'AccountingFxRate',
								o.DestinationAmount/(o.SourceAmount*(1-isnull(ps.ExchangeRateSpread,0))) as 'AccountingFxRateWithoutSp',
								o.SourceAmount*(1-isnull(ps.ExchangeRateSpread,0)) as 'AccountingNetAmount',
								o.SourceAmount*isnull(ps.ExchangeRateSpread,0) as 'SpreadAmountSC',
								o.SubmissionDateDateUTC as SettledAt,
								o.CompletionDateUTC as CompletedAt,
								o.ReverseDate as ReversedAt,
								null as Vendor,
								null as ClientReferenceId,
								null as MarketPlaceFeeAmount,
								null as MarketPlaceFeeRate,
								null as MarketPlaceVATAmount,
								null as MarketPlaceVATRate,
								o.payerPaymentCode as PayerReferenceId,
								null as MarketPlaceExchangeRate,
								null as MarketPlaceFeeAmountUsd,
								null as MarketPlaceVatAmountUsd,
								null as ForwardedAt,
								null as LastPushedAt,
								case 
								when sc.Id='ARG' and tc.Id='ARG' then 20
								when sc.Id='ARG' then 21
								when sc.Id='CHL' and tc.Id='CHL' then 22
								else 23
								end as TransactionType

								from wallet.Operations o (NOLOCK)
								left join wallet.PaymentChannels pc on pc.id=o.PaymentChannelId
								left join wallet.PaymentChannelCurrencies pcc on pcc.PaymentChannelId=pc.Id
								left join wallet.Countries sc on sc.Id=o.SourceCountryId
								left join wallet.Countries tc on tc.Id=o.DestinationCountryId
								left join wallet.OperationPriceSnapshots ps (NOLOCK) on ps.id=o.PriceSnapshotId
								left join wallet.PaymentMethods pm on pm.id=pc.PaymentMethodId


								where o.CreatedDateUTC>=@dateFrom and o.CreatedDateUTC<@dateTo
								and o.OperationType='TOPUPS'
								and o.State in ('COMPLETED','REVERSED')
                                ";

			using (var connection = new SqlConnection(walletConnString))
			{
				connection.Open();
				SqlCommand command = new SqlCommand();
				command.Connection = connection;
				command.CommandTimeout = 600;
				dataWallet = connection.Query<TransactionalBaseDTO>(queryWallet, new { dateFrom, dateTo }).ToList();
				connection.Close();
			}
			using (var connection = new SqlConnection(moneyTransferConnString))
			{
				connection.Open();
				SqlCommand command = new SqlCommand();
				command.Connection = connection;
				command.CommandTimeout = 600;
				dataMoneyTransfer = connection.Query<TransactionalBaseDTO>(queryMoneyTransfer, new { dateFrom, dateTo }).ToList();
				connection.Close();
			}

			BulkOperations.UpsertData(dataMoneyTransfer, _configuration.GetConnectionString("MetricsConnString"), "dbo", "TransactionalBase", "Id");
			BulkOperations.UpsertData(dataWallet, _configuration.GetConnectionString("MetricsConnString"), "dbo", "TransactionalBase", "Id");
		}

		public void UpdateTCCountries()
		{
			var config = new MapperConfiguration(cfg => cfg.CreateMap<Country, Tccountry>());

			var mapper = config.CreateMapper();

			var countries = new List<Country>();

			using (var ctx = new RemiteeServicesTransactionCollectorDbContext(_configuration))
			{
				countries = ctx.Countries.ToList();
			}

			//Agrego paises de Ledger
			var parsedLedgerData = new List<Tccountry>();

			var queryLedger = @"select isoCode as Id,
						name as Description,
						isoCode as Isothree,
						code as Isotwo
						from Countries
                        ";
			using (var connection = new SqlConnection(_configuration.GetConnectionString("LedgerConnString")))
			{
				var data = new DataTable();
				connection.Open();
				SqlCommand command = new SqlCommand();
				command.Connection = connection;
				command.CommandText = queryLedger;
				SqlDataAdapter reader = new SqlDataAdapter(command);
				reader.Fill(data);
				parsedLedgerData = (from row in data.AsEnumerable()
									select new Tccountry(row)).ToList();
				connection.Close();
			}

			//Agrego paises de Wallet
			var parsedWalletData = new List<Tccountry>();

			var queryWallet = @"select id as Id,
						Name as Description,
						id as Isothree,
						countryCode as Isotwo
						from wallet.Countries
                        ";
			using (var connection = new SqlConnection(_configuration.GetConnectionString("WalletConnString")))
			{
				var data = new DataTable();
				connection.Open();
				SqlCommand command = new SqlCommand();
				command.Connection = connection;
				command.CommandText = queryWallet;
				SqlDataAdapter reader = new SqlDataAdapter(command);
				reader.Fill(data);
				parsedWalletData = (from row in data.AsEnumerable()
									select new Tccountry(row)).ToList();
				connection.Close();
			}

			var toUpdate = mapper.Map<List<Country>, List<Tccountry>>(countries);
			toUpdate.AddRange(parsedLedgerData);
			toUpdate.AddRange(parsedWalletData);
			var unique = toUpdate.DistinctBy(x => x.Id).ToList();
			BulkOperations.UpsertData(unique, _configuration.GetConnectionString("MetricsConnString"), "tc", "Tccountries", "Id");
		}

		public void UpdateTCTables(DateTime dateFrom)
		{
			//UpdateTCCountries();

			UpdateTCSenders(dateFrom);

			UpdateTCReceivers(dateFrom);

			UpdateTCTransactions(dateFrom);
		}

		private void UpdateTCReceivers(DateTime dateFrom)
		{
			var config = new MapperConfiguration(cfg => cfg.CreateMap<ModelsTC.Receiver, Tcreceiver>());

			var mapper = config.CreateMapper();

			var users = new List<ModelsTC.Receiver>();

			using (var ctx = new RemiteeServicesTransactionCollectorDbContext(_configuration))
			{
				users = ctx.Receivers.Where(x => ctx.Transactions.Where(y => y.DateCreated >= dateFrom && y.ReceiverId == x.AccountId).FirstOrDefault() != null).ToList();
			}

			var toUpdate = mapper.Map<List<ModelsTC.Receiver>, List<Tcreceiver>>(users);

			BulkOperations.UpsertDataByBatch(toUpdate, _configuration.GetConnectionString("MetricsConnString"), "tc", "Tcreceivers", "Id", 50000);

		}

		private void UpdateTCSenders(DateTime dateFrom)
		{
			var config = new MapperConfiguration(cfg => cfg.CreateMap<ModelsTC.Sender, Tcsender>());

			var mapper = config.CreateMapper();

			var users = new List<ModelsTC.Sender>();

			using (var ctx = new RemiteeServicesTransactionCollectorDbContext(_configuration))
			{
				users = ctx.Senders.Where(x => ctx.Transactions.Where(y => y.DateCreated >= dateFrom && y.SenderId == x.AccountId).FirstOrDefault() != null).ToList();
			}

			var toUpdate = mapper.Map<List<ModelsTC.Sender>, List<Tcsender>>(users);
			BulkOperations.UpsertDataByBatch(toUpdate, _configuration.GetConnectionString("MetricsConnString"), "tc", "Tcsenders", "Id", 50000);
		}

		public void UpdateTCTransactions(DateTime dateFrom)
		{
			var config = new MapperConfiguration(cfg => cfg.CreateMap<Transaction, Tctransaction>());

			var mapper = config.CreateMapper();

			var users = new List<Transaction>();

			using (var ctx = new RemiteeServicesTransactionCollectorDbContext(_configuration))
			{
				users = ctx.Transactions.Where(x => x.DateCreated >= dateFrom).ToList();
			}

			var toUpdate = mapper.Map<List<Transaction>, List<Tctransaction>>(users);
			BulkOperations.UpsertDataByBatch(toUpdate, _configuration.GetConnectionString("MetricsConnString"), "tc", "Tctransactions", "Id", 50000);

		}

		public void UpdateBasicChurn(List<int> countries, int year, int month)
		{
			string ledgerConnString = _configuration.GetConnectionString("LedgerConnString");
			var parsedData = new List<BasicChurn>();
			foreach (var country in countries)
			{
				var query = @"select @year as Year,
	                            @month as Month,
	                            3 as Churn,
	                            (select c.Name from Countries c where c.Id=@sourceCountryId) as CountryName,
	                            (select c.isoCode from Countries c where c.Id=@sourceCountryId) as CountryCode,
	                            COUNT(*) as Count
                            from Parties p
                            where exists(SELECT * FROM Transactions t WHERE t.sourceCountryId = @sourceCountryId AND t.senderPartyId = p.id AND t.[status] >= 3 AND YEAR(t.createdDate) = YEAR(DATEADD(MONTH, -1 *3, datetimefromparts(@year,@month,1,0,0,0,0))) AND MONTH(t.createdDate) = MONTH(DATEADD(MONTH, -1 *3, datetimefromparts(@year,@month,1,0,0,0,0))))
                            and not exists(SELECT * FROM Transactions t WHERE  t.sourceCountryId = @sourceCountryId AND t.senderPartyId = p.id AND t.[status] >= 3 AND t.createdDate >= DATEADD(MONTH, 1, DATEADD(MONTH, -1 *3, datetimefromparts(@year,@month,1,0,0,0,0))) and t.createdDate<DATEADD(MONTH, 1, datetimefromparts(@year,@month,1,0,0,0,0)))
                            union
                            select @year as Year,
	                            @month as Month,
	                            4 as Churn,
	                            (select c.Name from Countries c where c.Id=@sourceCountryId) as CountryName,
	                            (select c.isoCode from Countries c where c.Id=@sourceCountryId) as CountryCode,
	                            COUNT(*) as Count
                            from Parties p
                            where exists(SELECT * FROM Transactions t WHERE t.sourceCountryId = @sourceCountryId AND t.senderPartyId = p.id AND t.[status] >= 3 AND YEAR(t.createdDate) = YEAR(DATEADD(MONTH, -1 * 4, datetimefromparts(@year,@month,1,0,0,0,0))) AND MONTH(t.createdDate) = MONTH(DATEADD(MONTH, -1 * 4, datetimefromparts(@year,@month,1,0,0,0,0))))
                            and not exists(SELECT * FROM Transactions t WHERE  t.sourceCountryId = @sourceCountryId AND t.senderPartyId = p.id AND t.[status] >= 3 AND t.createdDate >= DATEADD(MONTH, 1, DATEADD(MONTH, -1 * 4, datetimefromparts(@year,@month,1,0,0,0,0))) and t.createdDate<DATEADD(MONTH, 1, datetimefromparts(@year,@month,1,0,0,0,0)))
                            union
                            select @year as Year,
	                            @month as Month,
	                            5 as Churn,
	                            (select c.Name from Countries c where c.Id=@sourceCountryId) as CountryName,
	                            (select c.isoCode from Countries c where c.Id=@sourceCountryId) as CountryCode,
	                            COUNT(*) as Count
                            from Parties p
                            where exists(SELECT * FROM Transactions t WHERE t.sourceCountryId = @sourceCountryId AND t.senderPartyId = p.id AND t.[status] >= 3 AND YEAR(t.createdDate) = YEAR(DATEADD(MONTH, -1 * 5, datetimefromparts(@year,@month,1,0,0,0,0))) AND MONTH(t.createdDate) = MONTH(DATEADD(MONTH, -1 * 5, datetimefromparts(@year,@month,1,0,0,0,0))))
                            and not exists(SELECT * FROM Transactions t WHERE  t.sourceCountryId = @sourceCountryId AND t.senderPartyId = p.id AND t.[status] >= 3 AND t.createdDate >= DATEADD(MONTH, 1, DATEADD(MONTH, -1 * 5, datetimefromparts(@year,@month,1,0,0,0,0))) and t.createdDate<DATEADD(MONTH, 1, datetimefromparts(@year,@month,1,0,0,0,0)))
                            union
                            select @year as Year,
	                            @month as Month,
	                            6 as Churn,
	                            (select c.Name from Countries c where c.Id=@sourceCountryId) as CountryName,
	                            (select c.isoCode from Countries c where c.Id=@sourceCountryId) as CountryCode,
	                            COUNT(*) as Count
                            from Parties p
                            where exists(SELECT * FROM Transactions t WHERE t.sourceCountryId = @sourceCountryId AND t.senderPartyId = p.id AND t.[status] >= 3 AND YEAR(t.createdDate) = YEAR(DATEADD(MONTH, -1 * 6, datetimefromparts(@year,@month,1,0,0,0,0))) AND MONTH(t.createdDate) = MONTH(DATEADD(MONTH, -1 * 6, datetimefromparts(@year,@month,1,0,0,0,0))))
                            and not exists(SELECT * FROM Transactions t WHERE  t.sourceCountryId = @sourceCountryId AND t.senderPartyId = p.id AND t.[status] >= 3 AND t.createdDate >= DATEADD(MONTH, 1, DATEADD(MONTH, -1 * 6, datetimefromparts(@year,@month,1,0,0,0,0))) and t.createdDate<DATEADD(MONTH, 1, datetimefromparts(@year,@month,1,0,0,0,0)))
                            ";
				using (var connection = new SqlConnection(ledgerConnString))
				{
					var data = new DataTable();
					connection.Open();
					SqlCommand command = new SqlCommand();
					command.Connection = connection;
					command.CommandTimeout = 600;
					command.CommandText = query;
					command.Parameters.Add("@year", SqlDbType.Int).Value = year;
					command.Parameters.Add("@month", SqlDbType.Int).Value = month;
					command.Parameters.Add("@sourceCountryId", SqlDbType.Int).Value = country;
					SqlDataAdapter reader = new SqlDataAdapter(command);
					reader.Fill(data);
					parsedData = (from row in data.AsEnumerable()
								  select new BasicChurn(Convert.ToInt32(row.ItemArray[0]),
								  Convert.ToInt32(row.ItemArray[1]),
								  Convert.ToInt16(row.ItemArray[2]),
								  row.ItemArray[3]?.ToString(),
								  row.ItemArray[4]?.ToString(),
								  Convert.ToInt32(row.ItemArray[5]))).ToList();
					connection.Close();
				}
				using (var ctx = new RemiteeServicesMetricsContext(_configuration))
				{
					ctx.BasicChurns.AddRange(parsedData);
					ctx.SaveChanges();
				}
			}

		}

		public void UpdateNewSenders(int year, int month)
		{
			string ledgerConnString = _configuration.GetConnectionString("LedgerConnString");
			var parsedData = new List<NewSender>();

			var query = @"select YEAR(ft.createdDate) as Year,
		                        MONTH(ft.createdDate) as Month,
		                        c.name as CountryName,
		                        c.isoCode as CountryCode,
		                        COUNT(*) as Count,
		                        'All' as Type
                        from Parties p
                        inner join Transactions ft on ft.id = (select top 1 t.id from Transactions t where t.senderPartyId = p.id and t.[status] >= 3 order by t.createdDate asc)
                        inner join Countries c ON c.id = ft.sourceCountryId
                        where YEAR(ft.createdDate) = @year AND MONTH(ft.createdDate) = @month
                        group by c.name,
                        YEAR(ft.createdDate), 
                        MONTH(ft.createdDate),
                        c.isoCode
                        union
                        select YEAR(ft.createdDate) as Year,
		                        MONTH(ft.createdDate) as Month,
		                        c.name as CountryName,
		                        c.isoCode as CountryCode,
		                        COUNT(*) as Count,
		                        'App' as Type
                        from Parties p
                        inner join Transactions ft on ft.id = (select top 1 t.id from Transactions t where t.senderPartyId = p.id and t.[status] >= 3 and t.sourceCode='REMITEE' order by t.createdDate asc)
                        inner join Countries c ON c.id = ft.sourceCountryId
                        where YEAR(ft.createdDate) = @year AND MONTH(ft.createdDate) = @month
                        group by c.name,
                        YEAR(ft.createdDate), 
                        MONTH(ft.createdDate),
                        c.isoCode
                        ";
			using (var connection = new SqlConnection(ledgerConnString))
			{
				var data = new DataTable();
				connection.Open();
				SqlCommand command = new SqlCommand();
				command.Connection = connection;
				command.CommandTimeout = 600;
				command.CommandText = query;
				command.Parameters.Add("@year", SqlDbType.Int).Value = year;
				command.Parameters.Add("@month", SqlDbType.Int).Value = month;
				SqlDataAdapter reader = new SqlDataAdapter(command);
				reader.Fill(data);
				parsedData = (from row in data.AsEnumerable()
							  select new NewSender(Convert.ToInt32(row.ItemArray[0]),
								   Convert.ToInt32(row.ItemArray[1]),
								   row.ItemArray[2]?.ToString(),
								   row.ItemArray[3]?.ToString(),
								   Convert.ToInt32(row.ItemArray[4]),
								   row.ItemArray[5]?.ToString())).ToList();
				connection.Close();
			}
			using (var ctx = new RemiteeServicesMetricsContext(_configuration))
			{
				ctx.NewSenders.AddRange(parsedData);
				ctx.SaveChanges();
			}


		}

		public void UpdateReceivers(int year, int month)
		{
			string ledgerConnString = _configuration.GetConnectionString("LedgerConnString");
			var parsedData = new List<Models.Receiver>();

			var query = @"select YEAR(ft.createdDate) as Year,
		                        MONTH(ft.createdDate) as Month,
		                        sc.name as CountryName,
		                        sc.isoCode as CountryCode,
		                        COUNT(DISTINCT p.phoneNumber) as Count,
		                        'All' as Type,
		                        null as Description
                        from Parties p
                        inner join Transactions ft on ft.id = (select top 1 t.id from Transactions t where t.recipientPartyId = p.id and t.[status] >= 3 order by t.createdDate asc)
                        inner join Countries sc on sc.id = ft.sourceCountryId
                        where YEAR(ft.createdDate)=@year and MONTH(ft.createdDate)=@month
                        group by sc.name,sc.isoCode, YEAR(ft.createdDate), MONTH(ft.createdDate)
                        union
                        select YEAR(ft.createdDate) as Year,
		                        MONTH(ft.createdDate) as Month,
		                        sc.name as CountryName,
		                        sc.isoCode as CountryCode,
		                        COUNT(DISTINCT p.phoneNumber) as Count,
		                        'All' as Type,
		                        'mt_beneficiarios_nuevos_mes_a_mes' as Description
                        from Parties p
                        inner join Transactions ft on ft.id = (select top 1 t.id from Transactions t where t.recipientPartyId = p.id and t.[status] >= 3 order by t.createdDate asc)
                        inner join Countries sc on sc.id = ft.sourceCountryId
                        where EXISTS(select * from transactions t where t.status >= 3 and t.recipientPartyId = p.id and t.collectMethod in (1,2))
                        --and EXISTS(select * from transactions t where t.status >= 3 and t.recipientPartyId = p.id and t.collectMethod in (8))
                        group by sc.name,sc.isoCode, YEAR(ft.createdDate), MONTH(ft.createdDate)
                        having YEAR(ft.createdDate)=@year  and MONTH(ft.createdDate)=@month
                        union
                        select YEAR(ft.createdDate) as Year,
		                        MONTH(ft.createdDate) as Month,
		                        sc.name as CountryName,
		                        sc.isoCode as CountryCode,
		                        COUNT(DISTINCT p.phoneNumber) as Count,
		                        'All' as Type,
		                        'topups_n_services_beneficiarios_mes_a_mes_sin_mt' as Description
                        from Parties p
                        inner join Transactions ft on ft.id = (select top 1 t.id from Transactions t where t.recipientPartyId = p.id and t.[status] >= 3 order by t.createdDate asc)
                        inner join Countries sc on sc.id = ft.sourceCountryId
                        where not EXISTS(select * from transactions t where t.status >= 3 and t.recipientPartyId = p.id and t.collectMethod in (1,2))
                        and EXISTS(select * from transactions t where t.status >= 3 and t.recipientPartyId = p.id and t.collectMethod not in (1,2))
                        group by sc.name,sc.isoCode, YEAR(ft.createdDate), MONTH(ft.createdDate)
                        having YEAR(ft.createdDate)=@year  and MONTH(ft.createdDate)=@month
                        order by sc.name, [year], [month]
                        ";
			using (var connection = new SqlConnection(ledgerConnString))
			{
				var data = new DataTable();
				connection.Open();
				SqlCommand command = new SqlCommand();
				command.Connection = connection;
				command.CommandTimeout = 600;
				command.CommandText = query;
				command.Parameters.Add("@year", SqlDbType.Int).Value = year;
				command.Parameters.Add("@month", SqlDbType.Int).Value = month;
				SqlDataAdapter reader = new SqlDataAdapter(command);
				reader.Fill(data);
				parsedData = (from row in data.AsEnumerable()
							  select new Models.Receiver(Convert.ToInt32(row.ItemArray[0]),
									 Convert.ToInt32(row.ItemArray[1]),
									 row.ItemArray[2]?.ToString(),
									 row.ItemArray[3]?.ToString(),
									 Convert.ToInt32(row.ItemArray[4]),
									 row.ItemArray[5]?.ToString(),
									 row.ItemArray[6]?.ToString())).ToList();
				connection.Close();
			}
			using (var ctx = new RemiteeServicesMetricsContext(_configuration))
			{
				ctx.Receivers.AddRange(parsedData);
				ctx.SaveChanges();
			}


		}

		public void UpdateRegistrating(int year, int month)
		{
			string ledgerConnString = _configuration.GetConnectionString("LedgerConnString");
			string walletConnString = _configuration.GetConnectionString("WalletConnString");
			var parsedData = new List<Registrating>();
			var dataLedger = new DataTable();
			var dataWallet = new DataTable();

			var queryLedger = @"select c.name as CountryName, 
	                                YEAR(u.dateCreated) as Year, 
	                                MONTH(u.dateCreated) as Month, 
	                                count(*) as CompletedCount,
	                                c.isoCode as CountryCode
                                from users u
                                inner join countries c on c.id = u.countryId
                                where u.Type=0 and YEAR(u.dateCreated)=@year and MONTH(u.dateCreated)=@month
                                GROUP BY c.name, YEAR(u.dateCreated), MONTH(u.dateCreated),c.isoCode

                        ";
			var queryWallet = @"select c.Name as CountryName,
		                                year(u.createdATUtc) as Year,
		                                month(u.CreatedAtUtc) as Month,
		                                count(u.id) as CompletedCount,
		                                c.Id as CountryCode
                                from wallet.Users u
                                left join wallet.Countries c on c.id=u.countryId
                                where year(u.createdATUtc)=@year and month(u.CreatedAtUtc)=@month
                                GROUP BY c.Id , year(u.createdATUtc), month(u.CreatedAtUtc),c.Name
                                order by [year], [month], c.Name,c.Id
                                ";
			using (var connection = new SqlConnection(walletConnString))
			{
				connection.Open();
				SqlCommand command = new SqlCommand();
				command.Connection = connection;
				command.CommandTimeout = 600;
				command.CommandText = queryWallet;
				command.Parameters.Add("@year", SqlDbType.Int).Value = year;
				command.Parameters.Add("@month", SqlDbType.Int).Value = month;
				SqlDataAdapter reader = new SqlDataAdapter(command);
				reader.Fill(dataWallet);

				connection.Close();
			}
			using (var connection = new SqlConnection(ledgerConnString))
			{
				connection.Open();
				SqlCommand command = new SqlCommand();
				command.Connection = connection;
				command.CommandText = queryLedger;
				command.Parameters.Add("@year", SqlDbType.Int).Value = year;
				command.Parameters.Add("@month", SqlDbType.Int).Value = month;
				SqlDataAdapter reader = new SqlDataAdapter(command);
				reader.Fill(dataLedger);

				connection.Close();
			}
			var tempData = dataLedger.AsEnumerable()
						  .Concat(dataWallet.AsEnumerable())
						  .GroupBy(x => new
						  {
							  countryName = x.Field<string>("CountryName"),
							  year = x.Field<int>("Year"),
							  month = x.Field<int>("Month"),
							  countryCode = x.Field<string>("CountryCode")
						  });
			foreach (var g in tempData)
			{
				parsedData.Add(new Registrating
				{
					Id = Guid.NewGuid(),
					CountryName = g.Key.countryName,
					CountryCode = g.Key.countryCode,
					Year = g.Key.year,
					Month = g.Key.month,
					CompletedCount = g.Sum(x => x.Field<int>("CompletedCount"))
				});
			}
			using (var ctx = new RemiteeServicesMetricsContext(_configuration))
			{
				ctx.Registratings.AddRange(parsedData.ToList());
				ctx.SaveChanges();
			}


		}

		public void UpdateSenders(int year, int month)
		{
			string ledgerConnString = _configuration.GetConnectionString("LedgerConnString");
			var parsedData = new List<Models.Sender>();

			var query = @"select year(t.createdDate) as Year, 
		                            month(t.createdDate) as Month,
		                            sc.name as CountryName,
		                            sc.isoCode as CountryCode,
		                            count(DISTINCT sp.documentNumber) as Count,
		                            'All' as Type
                            from Transactions t
                            inner join Parties sp on sp.id = t.senderPartyId
                            inner join Countries sc ON sc.id = t.sourceCountryId
                            where t.status >= 3 and year(t.createdDate)=@year and month(t.createdDate)=@month
                            group by sc.name, year(t.createdDate), month(t.createdDate), sc.isoCode
                            union
                            select year(t.createdDate) as Year, 
		                            month(t.createdDate) as Month,
		                            sc.name as CountryName,
		                            sc.isoCode as CountryCode,
		                            count(DISTINCT sp.documentNumber) as Count,
		                            'App' as Type
                            from Transactions t
                            inner join Parties sp on sp.id = t.senderPartyId
                            inner join Countries sc ON sc.id = t.sourceCountryId
                            where t.status >= 3 and year(t.createdDate)=@year and month(t.createdDate)=@month
                            and t.sourceCode='REMITEE'
                            group by sc.name, year(t.createdDate), month(t.createdDate), sc.isoCode
                            order by sc.name, Year, Month
                        ";
			using (var connection = new SqlConnection(ledgerConnString))
			{
				var data = new DataTable();
				connection.Open();
				SqlCommand command = new SqlCommand();
				command.Connection = connection;
				command.CommandTimeout = 600;
				command.CommandText = query;
				command.Parameters.Add("@year", SqlDbType.Int).Value = year;
				command.Parameters.Add("@month", SqlDbType.Int).Value = month;
				SqlDataAdapter reader = new SqlDataAdapter(command);
				reader.Fill(data);
				parsedData = (from row in data.AsEnumerable()
							  select new Models.Sender(Convert.ToInt32(row.ItemArray[0]),
									 Convert.ToInt32(row.ItemArray[1]),
									 row.ItemArray[2]?.ToString(),
									 row.ItemArray[3]?.ToString(),
									 Convert.ToInt32(row.ItemArray[4]),
									 row.ItemArray[5]?.ToString())).ToList();
				connection.Close();
			}
			using (var ctx = new RemiteeServicesMetricsContext(_configuration))
			{
				ctx.Senders.AddRange(parsedData);
				ctx.SaveChanges();
			}


		}

		public void UpdateSendersBreakdown(List<int> countries, int year, int month)
		{
			string ledgerConnString = _configuration.GetConnectionString("LedgerConnString");
			var parsedData = new List<SendersBreakdown>();
			foreach (var country in countries)
			{
				var query = @"select x.Year,
                            x.Month,
                            x.CountryName,
                            x.CountryCode,
                            sum(x.OnlyMTCount) as OnlyMTCount,
                            sum(x.OnlyTOPUPCount) as OnlyTOPUPCount,
                            sum(x.BOTHCount) as BOTHCount,
                            x.Type
                            from (
                            --only mt
                            select year(t.createdDate) as Year, 
	                            month(t.createdDate) as Month, 
	                            (select c.Name from Countries c where c.Id=@sourceCountryId) as CountryName,
	                            (select c.isoCode from Countries c where c.Id=@sourceCountryId) as CountryCode,
	                            count(DISTINCT sp.documentNumber) as OnlyMTCount,
	                            0 as OnlyTOPUPCount,
	                            0 as BOTHCount,
	                            'All' as Type
                            from Transactions t
                            inner join Parties sp on sp.id = t.senderPartyId
                            where t.status >= 3
                            and t.sourceCountryId = @sourceCountryId
                            and t.collectMethod IN (1,2)
                            and not EXISTS(select * from Transactions tp where tp.senderPartyId = sp.id and tp.status >= 3 and tp.collectMethod in (8) and year(tp.createdDAte) = year(t.createdDate) and month(tp.createdDAte) = month(t.createdDate))
                            group by year(t.createdDate), month(t.createdDate)
                            union
                            --only topup
                            select year(t.createdDate) as Year, 
	                            month(t.createdDate) as Month, 
	                            (select c.Name from Countries c where c.Id=@sourceCountryId) as CountryName,
	                            (select c.isoCode from Countries c where c.Id=@sourceCountryId) as CountryCode,
	                            0 as OnlyMTCount,
	                            count(DISTINCT sp.documentNumber) as OnlyTOPUPCount,
	                            0 as BOTHCount,
	                            'All' as Type
                            from Transactions t
                            inner join Parties sp on sp.id = t.senderPartyId
                            where t.status >= 3
                            and t.sourceCountryId = @sourceCountryId
                            and t.collectMethod IN (8)
                            and not EXISTS(select * from Transactions tp where tp.senderPartyId = sp.id and tp.status >= 3 and tp.collectMethod in (1,2) and year(tp.createdDAte) = year(t.createdDate) and month(tp.createdDAte) = month(t.createdDate))
                            group by year(t.createdDate), month(t.createdDate)
                            union
                            --both
                            select  year(t.createdDate) as Year, 
	                            month(t.createdDate) as Month, 
	                            (select c.Name from Countries c where c.Id=@sourceCountryId) as CountryName,
	                            (select c.isoCode from Countries c where c.Id=@sourceCountryId) as CountryCode,
	                            0 as OnlyMTCount,
	                            0 as OnlyTOPUPCount,
	                            count(DISTINCT sp.documentNumber) as BOTHCount,
	                            'All' as Type
                            from Transactions t
                            inner join Parties sp on sp.id = t.senderPartyId
                            where t.status >= 3
                            and t.sourceCountryId = @sourceCountryId
                            and t.collectMethod IN (8)
                            and EXISTS(select * from Transactions tp where tp.senderPartyId = sp.id and tp.status >= 3 and tp.collectMethod in (1,2) and year(tp.createdDAte) = year(t.createdDate) and month(tp.createdDAte) = month(t.createdDate))
                            group by year(t.createdDate), month(t.createdDate)
                            ) x
                            where x.Year = @year and x.Month = @month
                            group by  x.Year,
                            x.Month,
                            x.CountryName,
                            x.CountryCode,
                            x.Type
                            union
                            select x.Year,
                            x.Month,
                            x.CountryName,
                            x.CountryCode,
                            sum(x.OnlyMTCount) as OnlyMTCount,
                            sum(x.OnlyTOPUPCount) as OnlyTOPUPCount,
                            sum(x.BOTHCount) as BOTHCount,
                            x.Type
                            from (
                            --only mt
                            select year(t.createdDate) as Year, 
	                            month(t.createdDate) as Month, 
	                            (select c.Name from Countries c where c.Id=@sourceCountryId) as CountryName,
	                            (select c.isoCode from Countries c where c.Id=@sourceCountryId) as CountryCode,
	                            count(DISTINCT sp.documentNumber) as OnlyMTCount,
	                            0 as OnlyTOPUPCount,
	                            0 as BOTHCount,
	                            'App' as Type
                            from Transactions t
                            inner join Parties sp on sp.id = t.senderPartyId
                            where t.status >= 3
                            and t.sourceCountryId = @sourceCountryId
                            and t.collectMethod IN (1,2)
							and t.sourceCOde='REMITEE'
                            and not EXISTS(select * from Transactions tp where tp.senderPartyId = sp.id and tp.status >= 3 and tp.collectMethod in (8) and year(tp.createdDAte) = year(t.createdDate) and month(tp.createdDAte) = month(t.createdDate) and tp.sourceCode='REMITEE')
                            group by year(t.createdDate), month(t.createdDate)
                            union
                            --only topup
                            select year(t.createdDate) as Year, 
	                            month(t.createdDate) as Month, 
	                            (select c.Name from Countries c where c.Id=@sourceCountryId) as CountryName,
	                            (select c.isoCode from Countries c where c.Id=@sourceCountryId) as CountryCode,
	                            0 as OnlyMTCount,
	                            count(DISTINCT sp.documentNumber) as OnlyTOPUPCount,
	                            0 as BOTHCount,
	                            'App' as Type
                            from Transactions t
                            inner join Parties sp on sp.id = t.senderPartyId
                            where t.status >= 3
                            and t.sourceCountryId = @sourceCountryId
                            and t.collectMethod IN (8)
							and t.sourceCOde='REMITEE'
                            and not EXISTS(select * from Transactions tp where tp.senderPartyId = sp.id and tp.status >= 3 and tp.collectMethod in (1,2) and year(tp.createdDAte) = year(t.createdDate) and month(tp.createdDAte) = month(t.createdDate) and tp.sourceCode='REMITEE')
                            group by year(t.createdDate), month(t.createdDate)
                            union
                            --both
                            select  year(t.createdDate) as Year, 
	                            month(t.createdDate) as Month, 
	                            (select c.Name from Countries c where c.Id=@sourceCountryId) as CountryName,
	                            (select c.isoCode from Countries c where c.Id=@sourceCountryId) as CountryCode,
            	                            0 as OnlyMTCount,
	                            0 as OnlyTOPUPCount,
	                            count(DISTINCT sp.documentNumber) as BOTHCount,
	                            'App' as Type
                            from Transactions t
                            inner join Parties sp on sp.id = t.senderPartyId
                            where t.status >= 3
                            and t.sourceCountryId = @sourceCountryId
                            and t.collectMethod IN (8)
							and t.sourceCOde='REMITEE'
                            and EXISTS(select * from Transactions tp where tp.senderPartyId = sp.id and tp.status >= 3 and tp.collectMethod in (1,2) and year(tp.createdDAte) = year(t.createdDate) and month(tp.createdDAte) = month(t.createdDate) and tp.sourceCode='REMITEE')
                            group by year(t.createdDate), month(t.createdDate)
                            ) x
                            where x.Year = @year and x.Month = @month
                            group by  x.Year,
                            x.Month,
                            x.CountryName,
                            x.CountryCode,
                            x.Type
                            ";
				using (var connection = new SqlConnection(ledgerConnString))
				{
					var data = new DataTable();
					connection.Open();
					SqlCommand command = new SqlCommand();
					command.Connection = connection;
					command.CommandTimeout = 600;
					command.CommandText = query;
					command.Parameters.Add("@year", SqlDbType.Int).Value = year;
					command.Parameters.Add("@month", SqlDbType.Int).Value = month;
					command.Parameters.Add("@sourceCountryId", SqlDbType.Int).Value = country;
					SqlDataAdapter reader = new SqlDataAdapter(command);
					reader.Fill(data);
					parsedData = (from row in data.AsEnumerable()
								  select new SendersBreakdown(Convert.ToInt32(row.ItemArray[0]),
									 Convert.ToInt32(row.ItemArray[1]),
									 row.ItemArray[2]?.ToString(),
									 row.ItemArray[3]?.ToString(),
									 Convert.ToInt32(row.ItemArray[4]),
									 Convert.ToInt32(row.ItemArray[5]),
									 Convert.ToInt32(row.ItemArray[6]),
									 row.ItemArray[7]?.ToString())).ToList();
					connection.Close();
				}
				using (var ctx = new RemiteeServicesMetricsContext(_configuration))
				{
					ctx.SendersBreakdowns.AddRange(parsedData);
					ctx.SaveChanges();
				}
			}

		}

		public async void UpdateExchangeRatesFromDailyPosition(DateTime dateFrom, DateTime dateTo, SpreadSheetConnector conn)
		{

			var data = conn.ReadData("Comitentes!E2:NE2", conn.fileLocation);
			var rowFinder = conn.ReadData("Comitentes!D:D", conn.fileLocation);
			var index = rowFinder.FindIndex(x => x.First().ToString().Contains("TC promedio (incluye BITSO)")) + 1;
			data.AddRange(conn.ReadData("Comitentes!E" + index + ":NE" + index, conn.fileLocation));
			var parsedData = data.ParseExchangeRatesArs();
			var filteredData = parsedData.Where(x => x.Date >= dateFrom && x.Date < dateTo);
			var targetCountries = new DataTable();

			using (var ctx = new RemiteeServicesMetricsContext(_configuration))
			{
				ctx.Database.SetCommandTimeout(300);
				ctx.ExchangeRates.Where(x => x.Date >= dateFrom && x.Date < dateTo).ExecuteDelete();
				ctx.ExchangeRates.AddRange(filteredData);
				var entities = ctx.ChangeTracker.Entries().Where(x => x.State == EntityState.Added);
				foreach (var entity in entities)
				{
					if (Convert.ToDecimal(entity.Property("ExchangeRate1").CurrentValue) == Convert.ToDecimal(0))
					{
						if (DateTime.Parse(entity.Property("Date").CurrentValue.ToString()).Date == dateFrom)
						{
							entity.Property("ExchangeRate1").CurrentValue = ctx.ExchangeRates
							.Where(x => x.Date <= DateTime.Parse(entity.Property("Date").CurrentValue.ToString()).Date && x.TargetCurrency == "CCL")
							.OrderByDescending(x => x.Date)
							.First().ExchangeRate1;
						}
						else
						{
							var test1 = entities
							.Where(x => DateTime.Parse(x.Property("Date").CurrentValue.ToString()).Date <= DateTime.Parse(entity.Property("Date").CurrentValue.ToString()).Date
							&& x.Property("CountryCode").CurrentValue.ToString() == "ARG"
							&& (decimal)x.Property("ExchangeRate1").CurrentValue > Convert.ToDecimal(0))
							.OrderByDescending(x => DateTime.Parse(x.Property("Date").CurrentValue.ToString()).Date)
							.First().Property("ExchangeRate1").CurrentValue;

							var test = (decimal)test1;

							entity.Property("ExchangeRate1").CurrentValue = test;
						}

					}
				}
				ctx.SaveChanges();
			}
		}

		public async void UpdateExchangeRatesFromCurrencyLayer(DateTime dateFrom, DateTime dateTo, SpreadSheetConnector conn)
		{
			var targetCountries = new DataTable();

			
			string ledgerConnString = _configuration.GetConnectionString("LedgerConnString");
			string walletConnString = _configuration.GetConnectionString("WalletConnString");
			using (var connection = new SqlConnection(ledgerConnString))
			{
				connection.Open();
				SqlCommand command = new SqlCommand();
				command.Connection = connection;
				command.CommandText = @"select distinct c.isoCode, c.name, c.currencyCode
from Transactions t
inner join Countries c on c.id=t.targetCountryId
where abs(t.status)>=3
and t.createdDate >= @dateFrom
and t.createdDate < @dateTo";
				command.Parameters.Add("@dateFrom", SqlDbType.Date).Value = dateFrom;
				command.Parameters.Add("@dateTo", SqlDbType.Date).Value = dateTo;
				SqlDataAdapter reader = new SqlDataAdapter(command);
				reader.Fill(targetCountries);

				connection.Close();
			}

			var targetCountriesList = targetCountries.AsEnumerable().DistinctBy(x => x.ItemArray[2].ToString()).ToList();
			foreach (DataRow row in targetCountriesList)
			{
				if (row.ItemArray[2].ToString() != "USD")
				{
					var response = GetCurrencyLayerExchangeRates(dateFrom, dateTo, row.ItemArray[2].ToString());
					response.Wait();
					if (!JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(response.Result)["success"])
					{
						continue;
					}
					var result = JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(response.Result)["quotes"];
					var list = JsonConvert.DeserializeObject<Dictionary<string, Dictionary<string, string>>>(Convert.ToString(result));
					var toUpdate = new List<ExchangeRate>();
					foreach (var item in list.Keys)
					{
						var x = list[item]["USD" + row.ItemArray[2].ToString()];
						toUpdate.Add(new ExchangeRate(row.ItemArray[1].ToString(), row.ItemArray[0].ToString(), DateTime.Parse(item).Date, decimal.Parse(list[item]["USD" + row.ItemArray[2].ToString()], new CultureInfo("en-US")), "USD", row.ItemArray[2].ToString()));
					}
					using (var ctx = new RemiteeServicesMetricsContext(_configuration))
					{
						ctx.ExchangeRates.AddRange(toUpdate);

						ctx.SaveChanges();
					}
				}

			}

		}

		public async Task<string?> GetCurrencyLayerExchangeRates(DateTime dateFrom, DateTime dateTo, string currency)
		{
			string? response = null;
			using (var client = new HttpClient())
			{
				response = await client.GetStringAsync(@"https://api.currencylayer.com/timeframe?access_key=55c7ec2cabff26b43e8eb956868f5e5d&currencies=" + currency + "&source=USD&start_date=" + dateFrom.ToString("yyyy-MM-dd") + "&end_date=" + dateTo.AddDays(-1).ToString("yyyy-MM-dd"));
			}
			return response;
		}

		public async void UpdatePartnersOperations(DateTime dateFrom, DateTime dateTo, SpreadSheetConnector conn)
		{

			Random rnd = new Random();


			//var interests = conn.ReadData("Comitentes!D3:NE3");

			var expenses = conn.ReadDataSheet("ARS", conn.fileLocation);
			var parsedExpenses = expenses.ParseExpenses(rnd, "ARS", dateFrom, dateTo);
			var filteredExpenses = parsedExpenses.Where(x => x.Date >= dateFrom && x.Date < dateTo);

			var clpExpenses = conn.ReadDataSheet("CLP", conn.fileLocation);
			var clpParsedExpenses = clpExpenses.ParseExpenses(rnd, "CLP", dateFrom, dateTo);
			var clpFilteredExpenses = clpParsedExpenses.Where(x => x.Date >= dateFrom && x.Date < dateTo);


			var exRateOf = new List<ExchangeRate>();
			using (var ctx = new RemiteeServicesMetricsContext(_configuration))
			{
				ctx.Database.SetCommandTimeout(300);
				exRateOf = ctx.ExchangeRates.Where(x => x.Date >= dateFrom && x.Date < dateTo && (x.TargetCurrency == "ARS" || x.TargetCurrency == "CLP")).ToList();
				ctx.PartnersOperations.Where(x => x.Date >= dateFrom && x.Date < dateTo && x.Partner == "REMITEE").ExecuteDelete();
				ctx.PartnersOperations.AddRange(filteredExpenses);
				ctx.PartnersOperations.AddRange(clpFilteredExpenses);

				ctx.SaveChanges();
			}

			var all = conn.ReadDataSheet("Comitentes", conn.fileLocation);
			var parsedExchanges = all.ParseExchanges(rnd, exRateOf, dateFrom, dateTo);
			var bitsoDataArs = conn.ReadDataSheet("ARS", conn.fileLocation);
			var bitsoDataUsd = conn.ReadDataSheet("USD", conn.fileLocation);

			var parsedBitsoExchanges = bitsoDataArs.ParseBitsoExchanges(bitsoDataUsd, rnd, exRateOf, dateFrom, dateTo);
			parsedExchanges.AddRange(parsedBitsoExchanges);

			var filteredExchanges = parsedExchanges.Where(x => x.Date >= dateFrom && x.Date < dateTo);
			using (var ctx = new RemiteeServicesMetricsContext(_configuration))
			{
				ctx.PartnersOperations.AddRange(filteredExchanges);

				ctx.SaveChanges();
			}

			var wires = conn.ReadDataSheet("Fond IB", conn.fileLocation);
			var parsedWires = wires.ParseIbPartnerWires(rnd);
			var filteredWires = parsedWires.Where(x => x.Date >= dateFrom && x.Date < dateTo);

			using (var ctx = new RemiteeServicesMetricsContext(_configuration))
			{
				ctx.Database.SetCommandTimeout(300);
				ctx.PartnersOperations.Where(x => x.Date >= dateFrom && x.Date < dateTo && x.Partner != "REMITEE").ExecuteDelete();
				ctx.PartnersOperations.AddRange(filteredWires);

				ctx.SaveChanges();
			}

		}

		public void UpdateTransactionalBaseClientReferenceId(DateTime dateFrom, DateTime dateTo)
        {
			var dataMoneyTransfer = new List<ClientReferenceIdDTO>();

			string moneyTransferConnString = _configuration.GetConnectionString("MoneyTransferConnString");

			var queryMoneyTransfer = @"select cast(p.Id as nvarchar(50)) as Id,
		p.ReferenceId as ClientReferenceId

								from mt.Payments p
								where p.Status in (3,4,5) and p.CreatedAt>=@dateFrom and p.CreatedAt<@dateTo
								--and qe.AmountUSD>0
                                ";
			using (var connection = new SqlConnection(moneyTransferConnString))
			{
				connection.Open();
				SqlCommand command = new SqlCommand();
				command.Connection = connection;
				command.CommandTimeout = 600;
				dataMoneyTransfer = connection.Query<ClientReferenceIdDTO>(queryMoneyTransfer, new { dateFrom, dateTo }).ToList();
				connection.Close();
			}

			BulkOperations.UpsertData(dataMoneyTransfer, _configuration.GetConnectionString("MetricsConnString"), "dbo", "TransactionalBase", "Id");
			
		}
	}
}
