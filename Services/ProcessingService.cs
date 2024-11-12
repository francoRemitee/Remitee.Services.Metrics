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
    public class ProcessingService
    {
        private readonly IConfigurationRoot _configuration;
        public ProcessingService(IConfigurationRoot configuration)
        {
            _configuration = configuration;
        }

        public void AddFixedDates(DateTime dateFrom, DateTime dateTo)
        {
            using (var ctx = new RemiteeServicesMetricsContext(_configuration))
            {
                using (SqlConnection conn = new SqlConnection(_configuration.GetConnectionString("MetricsConnString")))
                {
                    using (SqlCommand command = new SqlCommand("", conn))
                    {
                        conn.Open();
                        command.CommandTimeout = 600;
                        var updateTableCommand = @"UPDATE TransactionalBase SET CreatedAtFixed = CreatedAt,
						SettledAtFixed = SettledAt,
						CompletedAtFixed = CompletedAt,
						ReversedAtFixed = ReversedAt,
						ForwardedAtFixed = ForwardedAt,
						LastPushedAtFixed = LastPushedAt,
						StatusFixed = Status 
						where AccountingPeriod = @accountingPeriod";
                        command.CommandText = updateTableCommand;
                        command.Parameters.Add("@accountingPeriod", SqlDbType.NVarChar).Value = dateFrom.Year.ToString() + "-" + dateFrom.Month.ToString("00");
                        command.ExecuteNonQuery();
                    }

                }
            }
        }

        public void AddAccountingPeriod(int year, int month)
        {
            using (var ctx = new RemiteeServicesMetricsContext(_configuration))
            {
                using (SqlConnection conn = new SqlConnection(_configuration.GetConnectionString("MetricsConnString")))
                {
                    using (SqlCommand command = new SqlCommand("", conn))
                    {
                        conn.Open();
                        command.CommandTimeout = 600;
                        var updateTableCommand = @"UPDATE TransactionalBase SET AccountingPeriod = concat(@year,'-',@month) 
						where CreatedAt >= @dateFrom and CreatedAt < @dateTo
						and Status != 'reversed'
						";
                        command.CommandText = updateTableCommand;
                        command.Parameters.Add("@dateFrom", SqlDbType.Date).Value = new DateTime(year, month, 1);
                        command.Parameters.Add("@dateTo", SqlDbType.Date).Value = new DateTime(year, month, 1).AddMonths(1);
                        command.Parameters.Add("@year", SqlDbType.NVarChar).Value = year.ToString();
                        command.Parameters.Add("@month", SqlDbType.NVarChar).Value = month.ToString("00");
                        command.ExecuteNonQuery();
                    }

                }
            }
        }

        public void AddCountryNames()
        {
            var countriesList = new List<Tccountry>();

            using (var ctx = new RemiteeServicesMetricsContext(_configuration))
            {
                ctx.ChangeTracker.AutoDetectChangesEnabled = false;
                ctx.Database.SetCommandTimeout(300);

                countriesList = ctx.Tccountries.ToList();

            }
            DataTable dt = new DataTable("MyTable");
            dt = countriesList.ToDataTable();

            using (SqlConnection conn = new SqlConnection(_configuration.GetConnectionString("MetricsConnString")))
            {
                using (SqlCommand command = new SqlCommand("", conn))
                {
                    conn.Open();
                    command.CommandTimeout = 600;
                    var createTableCommand = "CREATE TABLE #TmpTable(Id nvarchar(50), Description nvarchar(100), Isothree nvarchar(3), isotwo nvarchar(2))";
                    command.CommandText = createTableCommand;
                    command.ExecuteNonQuery();

                    //Bulk insert into temp table
                    using (SqlBulkCopy bulkcopy = new SqlBulkCopy(conn))
                    {
                        bulkcopy.BulkCopyTimeout = 660;
                        bulkcopy.DestinationTableName = "#TmpTable";
                        bulkcopy.WriteToServer(dt);
                        bulkcopy.Close();
                    }

                    var updateTableCommand = "UPDATE T SET T.SourceCountryName = Tt.Description FROM TransactionalBase T INNER JOIN #TmpTable Tt ON T.SourceCountryCode=Tt.isothree and T.SourceCountryName is null;";
                    command.CommandText = updateTableCommand;
                    command.ExecuteNonQuery();

                    updateTableCommand = "UPDATE T SET T.TargetCountryName = Tt.Description FROM TransactionalBase T INNER JOIN #TmpTable Tt ON T.TargetCountryCode=Tt.isothree and T.TargetCountryName is null; DROP TABLE #TmpTable;";
                    command.CommandText = updateTableCommand;
                    command.ExecuteNonQuery();
                }
            }
        }

        public void AddBillingInfo(DateTime dateFrom, DateTime dateTo)
        {
            var billingTrx = new List<BillingDTO>();

            var billingQuery = @"SELECT 
v.TransactionId as LedgerId,
v.ClientName as BillingClientName,
v.ClientDocumentNumber as BillingClientDocumentNumber,
v.SiiFolio as BillingSiiFolio,
v.TotalAmount as BillingTotalAmount,
v.NetTaxed as BillingNetTaxed,
v.TaxAmount as BillingTaxAmount
  FROM [billing].[Voucher] v
  where v.dateCreated >= @dateFrom
  and v.dateCreated < @dateTo";

            using (var conn = new SqlConnection(_configuration.GetConnectionString("BillingConnString")))
            {
                billingTrx.AddRange(conn.Query<BillingDTO>(billingQuery, new { dateFrom = dateFrom, dateTo = dateTo }));
            }

            BulkOperations.UpdateData(billingTrx, _configuration.GetConnectionString("MetricsConnString"), "dbo", "TransactionalBase", "LedgerId");
        }

        public void AddArsRExchangeRate(DateTime dateFrom, DateTime dateTo)
        {
            var toUpdate = new List<ArsRExchangeRateDTO>();
            var amount = Convert.ToDecimal(_configuration.GetSection("AppSettings:ArsRExchangeRate:" + dateFrom.ToString("yyyy-MM-dd") + ":Balance").Value, CultureInfo.InvariantCulture);
            var exRate = Convert.ToDecimal(_configuration.GetSection("AppSettings:ArsRExchangeRate:" + dateFrom.ToString("yyyy-MM-dd") + ":Rate").Value, CultureInfo.InvariantCulture);
            decimal exRateOf;
            decimal.TryParse(_configuration.GetSection("AppSettings:ArsExchangeRate:" + dateFrom.ToString("yyyy-MM-dd") + ":Rate").Value, NumberStyles.Any, CultureInfo.InvariantCulture, out exRateOf);

            using (var ctx = new RemiteeServicesMetricsContext(_configuration))
            {
                ctx.Database.SetCommandTimeout(new TimeSpan(0, 5, 0));
                var data = ctx.TransactionalBases
                                   .Where(tb => tb.CreatedAt >= dateFrom
                                       && tb.CreatedAt < dateTo
                                       && tb.Status != "REVERSED"
                                       && tb.CollectMethod != "TOPUP"
                                       && (tb.SourceCurrency == "ARS" || tb.TargetCurrency == "ARS")
                                       && !(tb.SourceCurrency == "ARS" && tb.TargetCurrency == "ARS"))
                                   .Select(tb => new PartnerOperationDTO
                                   {
                                       Id = tb.Id,
                                       CreatedAt = tb.CreatedAt,
                                       Type =
                                                tb.TargetCurrency == tb.SourceCurrency ? null :
                                                tb.SourceCurrency == "ARS" ? "CREDIT" : "DEBIT"
                                              ,
                                       Description = (string?)(tb.SourceCurrency + " - " + tb.TargetCurrency),
                                       SourceCurrency = tb.SourceCurrency,
                                       TargetCurrency = tb.TargetCurrency,
                                       Amount = (
                                                   tb.TargetCurrency == "ARS" ? tb.TargetAmountTc : tb.GrossAmountSc
                                                ) ?? 0,
                                       ExchangeRate =
                                                   tb.TargetCurrency == tb.SourceCurrency ? null :
                                                   tb.SourceCurrency == "ARS" ? tb.ExchangeRateSc / (1 - tb.SpreadRate) ?? tb.GrossAmountSc / tb.NetAmountUsd :
                                                   tb.TargetAmountTc / (tb.GrossAmountSc / tb.ExchangeRateSc)
                                                ,
                                       ExchangeRateOf =
                                                   tb.TargetCurrency == tb.SourceCurrency ? null :
                                                   tb.SourceCurrency == "ARS" ? tb.ExchangeRateSc == null ? tb.MarketExchangeRate : tb.ExchangeRateSc :
                                                   tb.MarketExchangeRate

                                   }).ToList();
                data.AddRange(ctx.PartnersOperations
                                   .Where(po => po.Date >= dateFrom
                                           //&& po.Date < dateTo
                                           //&& po.Date.Month == month
                                           && (po.SourceCurrency == "ARS" || po.TargetCurrency == "ARS"))
                                           .Select(po => new PartnerOperationDTO
                                           {
                                               Id = po.Id.ToString(),
                                               CreatedAt = po.Date,
                                               Type = po.Type,
                                               Description = po.Description,
                                               SourceCurrency = po.SourceCurrency,
                                               TargetCurrency = po.TargetCurrency,
                                               Amount = po.Amount,
                                               ExchangeRate = po.ExchangeRate,
                                               ExchangeRateOf = po.ExchangeRateOf
                                           }));
                data.Add(new PartnerOperationDTO
                {
                    Id = "Beggining Balance",
                    CreatedAt = dateFrom,
                    Type = (string?)"CREDIT",
                    Description = (string?)"Origin Credit",
                    SourceCurrency = (string?)"ARS",
                    TargetCurrency = null,
                    Amount = amount,
                    ExchangeRate = exRate,
                    ExchangeRateOf = exRateOf
                });
                data.Sort((x,y) => x.CreatedAt.CompareTo(y.CreatedAt));


                decimal preBalance = 0;
                decimal postBalance = 0;
                decimal? rate = 0;
                decimal? rateOf = 0;

                for (int i = 0; i < data.Count(); i++)
                {
                    if (data.ElementAt(i).CreatedAt > dateTo)
                    {
                        break;
                    }
                    preBalance = postBalance;
                    var elementAti = data.ElementAt(i);
                    if (data.ElementAt(i).Type == "CREDIT")
                    {
                        postBalance += data.ElementAt(i).Amount;
                        if (data.ElementAt(i).Amount > 0)
                        {
                            rate = (preBalance * rate + data.ElementAt(i).Amount * (data.ElementAt(i).Description == "Intereses" || data.ElementAt(i).Description == "Pago a Proveedores" || data.ElementAt(i).Description == "BIND FCI" || data.ElementAt(i).ExchangeRate == null ? rate : data.ElementAt(i).ExchangeRate)) / postBalance;
                            rateOf = (preBalance * rateOf + data.ElementAt(i).Amount * (data.ElementAt(i).Description == "Intereses" || data.ElementAt(i).Description == "Pago a Proveedores" || data.ElementAt(i).Description == "BIND FCI" || data.ElementAt(i).ExchangeRateOf == null ? rateOf : data.ElementAt(i).ExchangeRateOf)) / postBalance;
                        }
                        toUpdate.Add(new ArsRExchangeRateDTO
                        {
                            Id = data.ElementAt(i).Id,
                            ArsrexchangeRate = rate,
                            ArsExchangeRateOf = rateOf
                        });
                    }
                    else if (data.ElementAt(i).Type == "DEBIT")
                    {

                        if (postBalance - data.ElementAt(i).Amount < 0)
                        {
                            var remainder = data.ElementAt(i).Amount - preBalance;
                            var singleRate = rate * preBalance;
                            var singleRateOf = rateOf * preBalance;
                            postBalance = 0;
                            for (int j = i + 1; j < data.Count(); j++)
                            {
                                var test = data.ElementAt(j);
                                if (data.ElementAt(j).Type == "CREDIT"
                                    && data.ElementAt(j).Amount > 0
                                    && data.ElementAt(j).Description != "Intereses"
                                    && data.ElementAt(j).Description != "Pago a Proveedores"
                                    && data.ElementAt(j).Description != "BIND FCI")
                                {
                                    if (remainder - data.ElementAt(j).Amount > 0)
                                    {
                                        var localSingleRate = data.ElementAt(j).Amount * data.ElementAt(j).ExchangeRate;
                                        singleRate += data.ElementAt(j).Amount * data.ElementAt(j).ExchangeRate;
                                        singleRateOf += data.ElementAt(j).Amount * data.ElementAt(j).ExchangeRateOf;
                                        remainder -= data.ElementAt(j).Amount;
                                        data.ElementAt(j).Amount = 0;
                                    }
                                    else
                                    {
                                        singleRate += remainder * data.ElementAt(j).ExchangeRate;
                                        singleRateOf += remainder * data.ElementAt(j).ExchangeRateOf;
                                        data.ElementAt(j).Amount -= remainder;
                                        remainder = 0;
                                        break;
                                    }
                                }
                            }

                            toUpdate.Add(new ArsRExchangeRateDTO
                            {
                                Id = data.ElementAt(i).Id,
                                ArsrexchangeRate = singleRate / data.ElementAt(i).Amount,
                                ArsExchangeRateOf = singleRateOf / data.ElementAt(i).Amount
                            });
                            var finalRate = singleRate / data.ElementAt(i).Amount;
                        }
                        else
                        {
                            postBalance -= data.ElementAt(i).Amount;
                            toUpdate.Add(new ArsRExchangeRateDTO
                            {
                                Id = data.ElementAt(i).Id,
                                ArsrexchangeRate = rate,
                                ArsExchangeRateOf = rateOf
                            });
                        }

                    }
                    else
                    {
                        toUpdate.Add(new ArsRExchangeRateDTO
                        {
                            Id = data.ElementAt(i).Id,
                            ArsrexchangeRate = rate,
                            ArsExchangeRateOf = rateOf
                        });
                    }

                    if (i < data.Count() - 1 && data.ElementAt(i).CreatedAt.Month < data.ElementAt(i + 1).CreatedAt.Month)
                    {
                        SettingsController.AddOrUpdateAppSetting("AppSettings:ArsRExchangeRate:" + dateTo.ToString("yyyy-MM-dd") + ":Balance", postBalance);
                        SettingsController.AddOrUpdateAppSetting("AppSettings:ArsRExchangeRate:" + dateTo.ToString("yyyy-MM-dd") + ":Rate", rate);
                        SettingsController.AddOrUpdateAppSetting("AppSettings:ArsExchangeRate:" + dateTo.ToString("yyyy-MM-dd") + ":Balance", postBalance);
                        SettingsController.AddOrUpdateAppSetting("AppSettings:ArsExchangeRate:" + dateTo.ToString("yyyy-MM-dd") + ":Rate", rateOf);
                    }
                }
                var settings = new SettingsController();

                SettingsController.AddOrUpdateAppSetting("AppSettings:ArsRExchangeRate:" + dateTo.ToString("yyyy-MM-dd") + ":Balance", postBalance);
                SettingsController.AddOrUpdateAppSetting("AppSettings:ArsRExchangeRate:" + dateTo.ToString("yyyy-MM-dd") + ":Rate", rate);
                SettingsController.AddOrUpdateAppSetting("AppSettings:ArsExchangeRate:" + dateTo.ToString("yyyy-MM-dd") + ":Balance", postBalance);
                SettingsController.AddOrUpdateAppSetting("AppSettings:ArsExchangeRate:" + dateTo.ToString("yyyy-MM-dd") + ":Rate", rateOf);
            }
            BulkOperations.UpdateDataByBatch(toUpdate, _configuration.GetConnectionString("MetricsConnString"), "dbo", "TransactionalBase", "Id", 50000);

        }

        public void AddClpRExchangeRate(DateTime dateFrom, DateTime dateTo)
        {

            var toUpdate = new List<ClpRExchangeRateDTO>();
            var amount = Convert.ToDecimal(_configuration.GetSection("AppSettings:ClpRExchangeRate:" + dateFrom.ToString("yyyy-MM-dd") + ":Balance").Value, CultureInfo.InvariantCulture);
            var exRate = Convert.ToDecimal(_configuration.GetSection("AppSettings:ClpRExchangeRate:" + dateFrom.ToString("yyyy-MM-dd") + ":Rate").Value, CultureInfo.InvariantCulture);
            decimal exRateOf;
            decimal.TryParse(_configuration.GetSection("AppSettings:ClpRExchangeRate:" + dateFrom.ToString("yyyy-MM-dd") + ":Rate").Value, NumberStyles.Any, CultureInfo.InvariantCulture, out exRateOf);

            using (var ctx = new RemiteeServicesMetricsContext(_configuration))
            {
                ctx.Database.SetCommandTimeout(new TimeSpan(0, 5, 0));
                var data = ctx.TransactionalBases
                                   .Where(tb => tb.CreatedAt >= dateFrom
                                       && tb.CreatedAt < dateTo
                                       && tb.Status != "REVERSED"
                                       && tb.CollectMethod != "TOPUP"
                                       && (tb.SourceCurrency == "CLP" || tb.TargetCurrency == "CLP")
                                       && !(tb.SourceCurrency == "CLP" && tb.TargetCurrency == "CLP"))
                                   .Select(tb => new PartnerOperationDTO
                                   {
                                       Id = tb.Id,
                                       CreatedAt = tb.CreatedAt,
                                       Type =
                                                tb.TargetCurrency == tb.SourceCurrency ? null :
                                                tb.SourceCurrency == "CLP" ? "CREDIT" : "DEBIT"
                                              ,
                                       Description = (string?)(tb.SourceCurrency + " - " + tb.TargetCurrency),
                                       SourceCurrency = tb.SourceCurrency,
                                       TargetCurrency = tb.TargetCurrency,
                                       Amount = (
                                                   tb.TargetCurrency == "CLP" ? tb.TargetAmountTc : tb.GrossAmountSc
                                                ) ?? 0,
                                       ExchangeRate =
                                                   tb.TargetCurrency == tb.SourceCurrency ? null :
                                                   tb.SourceCurrency == "CLP" ? tb.ExchangeRateSc / (1 - tb.SpreadRate) ?? tb.GrossAmountSc / tb.NetAmountUsd :
                                                   tb.TargetAmountTc / (tb.GrossAmountSc / tb.ExchangeRateSc)
                                                ,
                                       ExchangeRateOf =
                                                   tb.TargetCurrency == tb.SourceCurrency ? null :
                                                   tb.SourceCurrency == "CLP" ? tb.ExchangeRateSc == null ? tb.MarketExchangeRate : tb.ExchangeRateSc :
                                                   tb.MarketExchangeRate

                                   }).ToList();
                data.AddRange(ctx.PartnersOperations
                                   .Where(po => po.Date >= dateFrom
                                           //&& po.Date < dateTo
                                           //&& po.Date.Month == month
                                           && (po.SourceCurrency == "CLP" || po.TargetCurrency == "CLP"))
                                           .Select(po => new PartnerOperationDTO
                                           {
                                               Id = po.Id.ToString(),
                                               CreatedAt = po.Date,
                                               Type = po.Type,
                                               Description = po.Description,
                                               SourceCurrency = po.SourceCurrency,
                                               TargetCurrency = po.TargetCurrency,
                                               Amount = po.Amount,
                                               ExchangeRate = po.ExchangeRate,
                                               ExchangeRateOf = po.ExchangeRateOf
                                           }));
                data.Add(new PartnerOperationDTO
                {
                    Id = "Beggining Balance",
                    CreatedAt = dateFrom,
                    Type = (string?)"CREDIT",
                    Description = (string?)"Origin Credit",
                    SourceCurrency = (string?)"CLP",
                    TargetCurrency = null,
                    Amount = amount,
                    ExchangeRate = exRate,
                    ExchangeRateOf = exRateOf
                });
                var transactions = data.OrderBy(x => x.CreatedAt);


                decimal preBalance = 0;
                decimal postBalance = 0;
                decimal? rate = 0;

                for (int i = 0; i < transactions.Count(); i++)
                {
                    if (transactions.ElementAt(i).CreatedAt > dateTo)
                    {
                        break;
                    }
                    preBalance = postBalance;
                    var elementAti = transactions.ElementAt(i);
                    if (transactions.ElementAt(i).Type == "CREDIT")
                    {
                        postBalance += transactions.ElementAt(i).Amount;
                        if (transactions.ElementAt(i).Amount > 0)
                        {
                            rate = (preBalance * rate + transactions.ElementAt(i).Amount * (transactions.ElementAt(i).Description == "Intereses" || transactions.ElementAt(i).Description == "Pago a Proveedores" || transactions.ElementAt(i).ExchangeRate == null ? rate : transactions.ElementAt(i).ExchangeRate)) / postBalance;
                        }
                        toUpdate.Add(new ClpRExchangeRateDTO
                        {
                            Id = transactions.ElementAt(i).Id,
                            ClprexchangeRate = rate
                        });
                    }
                    else if (transactions.ElementAt(i).Type == "DEBIT")
                    {

                        if (postBalance - transactions.ElementAt(i).Amount < 0)
                        {
                            var remainder = transactions.ElementAt(i).Amount - preBalance;
                            var singleRate = rate * preBalance;
                            postBalance = 0;
                            for (int j = i + 1; j < transactions.Count(); j++)
                            {
                                if (transactions.ElementAt(j).Type == "CREDIT"
                                    && transactions.ElementAt(j).Amount > 0
                                    && transactions.ElementAt(j).Description != "Intereses"
                                    && transactions.ElementAt(j).Description != "Pago a Proveedores")
                                {
                                    if (remainder - transactions.ElementAt(j).Amount > 0)
                                    {
                                        var localSingleRate = transactions.ElementAt(j).Amount * transactions.ElementAt(j).ExchangeRate;
                                        singleRate += transactions.ElementAt(j).Amount * transactions.ElementAt(j).ExchangeRate;
                                        remainder -= transactions.ElementAt(j).Amount;
                                        transactions.ElementAt(j).Amount = 0;
                                    }
                                    else
                                    {
                                        singleRate += remainder * transactions.ElementAt(j).ExchangeRate;
                                        transactions.ElementAt(j).Amount -= remainder;
                                        remainder = 0;
                                        break;
                                    }
                                }
                            }

                            toUpdate.Add(new ClpRExchangeRateDTO
                            {
                                Id = transactions.ElementAt(i).Id,
                                ClprexchangeRate = singleRate / transactions.ElementAt(i).Amount
                            });
                            var finalRate = singleRate / transactions.ElementAt(i).Amount;
                        }
                        else
                        {
                            postBalance -= transactions.ElementAt(i).Amount;
                            toUpdate.Add(new ClpRExchangeRateDTO
                            {
                                Id = transactions.ElementAt(i).Id,
                                ClprexchangeRate = rate
                            });
                        }

                    }
                    else
                    {
                        toUpdate.Add(new ClpRExchangeRateDTO
                        {
                            Id = transactions.ElementAt(i).Id,
                            ClprexchangeRate = rate
                        });
                    }

                    if (i < transactions.Count() - 1 && transactions.ElementAt(i).CreatedAt.Month < transactions.ElementAt(i + 1).CreatedAt.Month)
                    {
                        SettingsController.AddOrUpdateAppSetting("AppSettings:ClpRExchangeRate:" + dateTo.ToString("yyyy-MM-dd") + ":Balance", postBalance);
                        SettingsController.AddOrUpdateAppSetting("AppSettings:ClpRExchangeRate:" + dateTo.ToString("yyyy-MM-dd") + ":Rate", rate);
                    }
                }
                var settings = new SettingsController();

                SettingsController.AddOrUpdateAppSetting("AppSettings:ClpRExchangeRate:" + dateTo.ToString("yyyy-MM-dd") + ":Balance", postBalance);
                SettingsController.AddOrUpdateAppSetting("AppSettings:ClpRExchangeRate:" + dateTo.ToString("yyyy-MM-dd") + ":Rate", rate);
            }
            BulkOperations.UpdateDataByBatch(toUpdate, _configuration.GetConnectionString("MetricsConnString"), "dbo", "TransactionalBase", "Id", 50000);

        }

        public void AddPayersReferences(DateTime dateFrom, DateTime dateTo)
        {
            List<PayerReference> payersRecords = new List<PayerReference>();
            var connStrings = _configuration.GetRequiredSection("PayersConnectionStrings").AsEnumerable();
            Dictionary<string, string> payerQuoteQuery = new Dictionary<string, string>();
            payerQuoteQuery.Add("PayersConnectionStrings:Cobru", @"SELECT r.transactionId as LedgerId
	                                  ,json_value(Json,'$.pk') as PayerReferenceId
	  
                                  FROM [dbo].[JsonRequestsResponses] rr
                                  left join Requests r on r.id=rr.requestId
                                  where method like 'GET /thirdpartywithdraw%'
                                  and rr.CreateDate between dateadd(day,-1,@dateFrom) and dateadd(day,1,@dateTo)
                                  order by rr.CreateDate desc");
            payerQuoteQuery.Add("PayersConnectionStrings:RippleNetCloud", @"SELECT [InternalId] as LedgerId
	                                  ,PaymentId as PayerReferenceId
                                  FROM [messaging].[Requests]
                                  where CreatedDate between dateadd(day,-1,@dateFrom) and dateadd(day,1,@dateTo)
                                  and ReceivingAddress like '%b89'
                                  order by CreatedDate desc");
            payerQuoteQuery.Add("PayersConnectionStrings:Cobre", @"
drop table if exists #FilteredJsonRequestsResponses
drop table if exists #FilteredJsonTransactions
 
 
-- Step 1: Filter the JsonRequestsResponses table
SELECT 
    rr.RequestId,
    rr.[Json],
	rr.payload,
    rr.Method,
    rr.CreateDate,
    ROW_NUMBER() OVER (PARTITION BY rr.RequestId ORDER BY rr.CreateDate ASC) AS rn
INTO #FilteredJsonRequestsResponses
FROM [dbo].[JsonRequestsResponses] rr WITH (NOLOCK)
WHERE rr.Method LIKE 'GET%'
AND rr.CreateDate BETWEEN DATEADD(DAY, -5, @dateFrom) AND DATEADD(DAY, 5, @dateTo);
 

 
-- Step 2: Filter the JsonTransactions table
SELECT 
    jt.CreateDate,
	jt.[Json],
    json_value(jt.[Json], '$.RequestId') AS RequestId
INTO #FilteredJsonTransactions 
FROM [dbo].[JsonTransactions] jt WITH (NOLOCK)
WHERE jt.CreateDate BETWEEN DATEADD(DAY, -1, @dateFrom) AND DATEADD(DAY, 1, @dateTo);
 

 
SELECT r.TransactionId as LedgerId,
					json_value(x.payload,'$.massivePayment.uuid') as PayerReferenceId
					  FROM #FilteredJsonTransactions jt
					  left join Requests r on upper(r.DtoRequestId)=jt.RequestId
					  left join #FilteredJsonRequestsResponses x on x.RequestId=r.Id and x.rn=1
					  where ISJSON(x.Payload) = 1
					  order by jt.CreateDate desc;

drop table if exists #FilteredJsonRequestsResponses
drop table if exists #FilteredJsonTransactions ");
            payerQuoteQuery.Add(@"PayersConnectionStrings:Ligo", @"SELECT distinct
								r.TransactionId as LedgerId,
								case when jrr.json ='""""' then null
								when json_value(jrr.Json,'$.status')='false' then null
								else json_value(jrr.Json,'$.data.uuid') end as PayerReferenceId
									FROM [dbo].[Requests] r
									left join JsonTransactions jt on jt.Id=r.JsonTransactionDtoId
									left join JsonRequestsResponses jrr on jrr.RequestId=r.Id
									where r.CreateDate between dateadd(day,-1,@dateFrom) and dateadd(day,1,@dateTo)");

            foreach (var connString in connStrings)
            {
                if (connString.Value != null && payerQuoteQuery.ContainsKey(connString.Key))
                {
                    using (var connection = new SqlConnection(connString.Value))
                    {
                        connection.Open();
                        using (SqlCommand command = new SqlCommand())
                        {
                            var result = connection.Query<PayerReference>(payerQuoteQuery[connString.Key], new { dateFrom, dateTo }, null, true, 6000 );
                            payersRecords.AddRange(result);

                            connection.Close();
                        }
                            
                    }
                }

            }
            BulkOperations.UpdateData(payersRecords, _configuration.GetConnectionString("MetricsConnString"), "dbo", "TransactionalBase", "LedgerId");

        }

        public void ModifyMarketExchangeRate(DateTime dateFrom, DateTime dateTo, SpreadSheetConnector conn)
        {
            using (var ctx = new RemiteeServicesMetricsContext(_configuration))
            {
                var exRates = new List<MarketExchangeRatesDTO>();
                decimal exRateCcl = 0;
                decimal exRateClp = 0;
                decimal exRateArs = 0;

                decimal? exRateMarket = 0;
                for (var dt = dateFrom; dt < dateTo; dt = dt.AddDays(1))
                {
                    exRateCcl = ctx.ExchangeRates.Where(x => x.Date.Date == dt.Date && x.TargetCurrency == "CCL").FirstOrDefault()?.ExchangeRate1 ?? throw new Exception("Missing CCL ExchangeRate for " + dt.ToString("dd/MM/yyyy"));
                    exRateClp = ctx.ExchangeRates.Where(x => x.Date.Date == dt.Date && x.TargetCurrency == "CLP").FirstOrDefault()?.ExchangeRate1 ?? throw new Exception("Missing CLP ExchangeRate for " + dt.ToString("dd/MM/yyyy"));
                    exRateArs = ctx.ExchangeRates.Where(x => x.Date.Date == dt.Date && x.TargetCurrency == "ARS").FirstOrDefault()?.ExchangeRate1 ?? throw new Exception("Missing ARS ExchangeRate for " + dt.ToString("dd/MM/yyyy"));
                    var currencies = ctx.ExchangeRates.Where(x => x.Date.Date == dt.Date && x.TargetCurrency != "CCL").Select(x => x.TargetCurrency).ToList();

                    foreach (var currency in currencies)
                    {
                        exRateMarket = ctx.ExchangeRates.Where(x => x.Date.Date == dt.Date && x.TargetCurrency == currency).FirstOrDefault()?.ExchangeRate1; //?? throw new Exception("Missing " + currency + " ExchangeRate for " + dt.ToString("dd/MM/yyyy"));
                        exRates.AddRange(ctx.TransactionalBases
                            .Where(x => x.CreatedAt.Date == dt.Date && x.TargetCurrency == currency)
                            .Select(x => new MarketExchangeRatesDTO
                            {
                                Id = x.Id,
                                ArsExchangeRate = exRateCcl,
                                ClpExchangeRate = exRateClp,
                                ArsExchangeRateOf = exRateArs,
                                MarketExchangeRate = x.MarketExchangeRate ?? exRateMarket
                            }));
                    }
                    exRates.AddRange(ctx.TransactionalBases
                        .Where(x => x.CreatedAt.Date == dt.Date && x.TargetCurrency == "USD")
                        .Select(x => new MarketExchangeRatesDTO
                        {
                            Id = x.Id,
                            ArsExchangeRate = exRateCcl,
                            ClpExchangeRate = exRateClp,
                            ArsExchangeRateOf = exRateArs,
                            MarketExchangeRate = 1
                        }));
                }
                BulkOperations.UpdateData(exRates, _configuration.GetConnectionString("MetricsConnString"), "dbo", "TransactionalBase", "Id");

            }
        }

        public void ModifyDates(DateTime dateFrom, DateTime dateTo)
        {
            List<DatesDTO> datesUpdate = new List<DatesDTO>();

            var moneyTransferQuery = @"select cast(p.Id as nvarchar(50)) as Id,
				p.TransactionId as LedgerId,
				p.CreatedAt,
				p.SettledAt,
				p.CompletedAt,
				p.ReversedAt,
				p.ForwardedAt,
				o.Date as LastPushedAt,
				case p.Status
					when 5 then 'REVERSED'
					when 4 then 'COMPLETED'
					when 3 then 'SETTLED'
					end as Status

				from mt.Payments p (NOLOCK)
				LEFT JOIN (SELECT *, row_number() OVER (PARTITION BY paymentId ORDER BY Date DESC) as rn from mt.Operations x (NOLOCK)) o on o.PaymentId = p.id and o.rn=1
				where p.Status in (3,4,5) and ((p.CreatedAt>=@dateFrom and p.CreatedAt<@dateTo) or
				(p.ForwardedAt>=@dateFrom and p.ForwardedAt<@dateTo) or (p.ReversedAt>=@dateFrom and p.ReversedAt<@dateTo)
				or (p.SettledAt>=@dateFrom and p.SettledAt<@dateTo) or (p.CompletedAt>=@dateFrom and p.CompletedAt<@dateTo) or (p.SettledAt>=@dateFrom and p.SettledAt<@dateTo)
				or (o.Date>=@dateFrom and o.Date<@dateTo))";
            var walletQuery = @"select cast(o.Id as nvarchar(50)) as Id,
				null as LedgerId,
				o.CreatedDateUTC as CreatedAt,
				o.SubmissionDateDateUTC as SettledAt,
				o.CompletionDateUTC as CompletedAt,
				o.ReverseDate as ReversedAt,
				null as ForwardedAt,
				null as LastPushedAt,
				o.State as Status
				from wallet.Operations o (NOLOCK)
				where ((o.CreatedDateUTC>=@dateFrom and o.CreatedDateUTC<@dateTo) or (o.SubmissionDateDateUTC>=@dateFrom and o.SubmissionDateDateUTC<@dateTo)
				or (o.CompletionDateUTC>=@dateFrom and o.CompletionDateUTC<@dateTo) or (o.ReverseDate>=@dateFrom and o.ReverseDate<@dateTo))
				and o.OperationType='TOPUPS'
				and o.State in ('COMPLETED','REVERSED')";

            using (var conn = new SqlConnection(_configuration.GetConnectionString("MoneyTransferConnString")))
            {
                conn.Open();
                SqlCommand command = new SqlCommand();
                command.Connection = conn;
                command.CommandTimeout = 600;
                command.Parameters.Add("@dateFrom", SqlDbType.Date).Value = dateFrom;
                command.Parameters.Add("@dateTo", SqlDbType.Date).Value = dateTo;
                datesUpdate.AddRange(conn.Query<DatesDTO>(moneyTransferQuery, new { dateFrom, dateTo }));
            }

            using (var conn = new SqlConnection(_configuration.GetConnectionString("WalletConnString")))
            {
                conn.Open();
                SqlCommand command = new SqlCommand();
                command.Connection = conn;
                command.CommandTimeout = 600;
                command.Parameters.Add("@dateFrom", SqlDbType.Date).Value = dateFrom;
                command.Parameters.Add("@dateTo", SqlDbType.Date).Value = dateTo;
                datesUpdate.AddRange(conn.Query<DatesDTO>(walletQuery, new { dateFrom, dateTo }));
            }

            BulkOperations.UpdateData(datesUpdate, _configuration.GetConnectionString("MetricsConnString"), "dbo", "TransactionalBase", "Id");

        }

        public void AddPayersDataFromConnectors(DateTime dateFrom, DateTime dateTo)
        {
            List<PaymentQuoteElements> payersRecords = new List<PaymentQuoteElements>();

            var connStrings = _configuration.GetRequiredSection("PayersConnectionStrings").AsEnumerable();
            Dictionary<string, string> payerQuoteQuery = new Dictionary<string, string>();
            payerQuoteQuery.Add("PayersConnectionStrings:DLocal", @"select [InternalTransactionId] as LedgerId
                                          ,[PayerExchangeRate]
                                          ,[PayerFee]
                                          ,[PayerFeeExpected]
										from dlocal.PaymentQuoteElements
										where createdDate >= dateadd(day,-7,@dateFrom)
										and createdDate < dateadd(day,7,@dateTo)");
            payerQuoteQuery.Add("PayersConnectionStrings:EasyPagos", @"select [InternalTransactionId] as LedgerId
                                      ,[PayerExchangeRate]
                                      ,[PayerFee]
										from Easypagos.PaymentQuoteElements
										where createdDate >= dateadd(day,-7,@dateFrom)
										and createdDate < dateadd(day,7,@dateTo)");
            payerQuoteQuery.Add("PayersConnectionStrings:Italcambio", @"select 
                                        i.TransactionId as LedgerId,
                                        case 
                                        when i.ExchangeRate<1
                                        then 1/i.ExchangeRate
                                        else i.ExchangeRate
                                        end as PayerExchangeRate,
                                        p.PayerFee,
                                        f.BaseFee as PayerFeeExpected
                                        from Italcambio.ItalcambioCashOutTransactions i
                                        left join Italcambio.PaymentQuoteElements p on p.InternalTransactionId=i.TransactionId
                                        left join Italcambio.PayerFees f on f.Currency=i.ReceiveCurrency and i.CreatedAt>=f.ValidFrom and i.CreatedAt<=isnull(f.ValidTo,@dateTo)
                                        where i.createdAt >= @dateFrom
                                        --and i.createdAt < @dateTo");
            payerQuoteQuery.Add("PayersConnectionStrings:LocalPayment", @"select [InternalTransactionId] as LedgerId
                                      ,[PayerExchangeRate]
                                      ,[PayerFee]
										from LocalPayment.PaymentQuoteElements
										where createdDate >= @dateFrom
										--and createdDate < @dateTo");
            payerQuoteQuery.Add("PayersConnectionStrings:Maxicambios", @"select [InternalTransactionId] as LedgerId
                                      ,[PayerExchangeRate]
                                      ,[PayerFee]
                                      ,[PayerFeeExpected]
                                      ,[PayerExchangeRateExpected]
										from messaging.PaymentQuoteElements
										where createdDate >= @dateFrom
										--and createdDate < dateadd(day,3,@dateTo)");
            payerQuoteQuery.Add("PayersConnectionStrings:Radar", @"select [InternalTransactionId] as LedgerId
                                  ,[PayerExchangeRate]
                                  ,[PayerFee]
                                  ,[PayerFeeExpected]
                                  ,[PayerExchangeRateExpected]
										from Radar.PaymentQuoteElements
										where createdDate >= dateadd(month,-1,@dateFrom)
										--and createdDate < dateadd(month,1,@dateTo)");
            payerQuoteQuery.Add("PayersConnectionStrings:RippleNetCloud", @"select 
                                        InternalTransactionId as LedgerId,
                                        isnull(nullif(PayerExchangeRate,0),1) as PayerExchangeRate,
                                        PayerFee
                                        from messaging.PaymentQuoteElements
										where createdDate >= @dateFrom
										--and createdDate < @dateTo");
            payerQuoteQuery.Add("PayersConnectionStrings:TransferZero", @"select [InternalTransactionId] as LedgerId
                                      ,[PayerExchangeRate]
                                      ,[PayerFee]
                                      ,[PayerFeeExpected]
                                      ,[PayerExchangeRateExpected]
										from TransferZero.PaymentQuoteElements
										where createdDate >= @dateFrom
										--and createdDate < @dateTo");
            payerQuoteQuery.Add("PayersConnectionStrings:BancoInter", @"select 
                                        TransactionId as LedgerId,
                                        PayerExchangeRate,
                                        PayerFee,
                                        PayerFeeExpected,
                                        PayerFeeExpectedUSD as RemiteeCalculatedFee
										from Inter.PaymentFeeAudit
										where createdDate >= @dateFrom
										--and createdDate < @dateTo");
            payerQuoteQuery.Add("PayersConnectionStrings:Coinag", @"select [InternalTransactionId] as LedgerId
                                          ,[PayerExchangeRate]
                                          ,[PayerFee]
                                          ,[PayerFeeExpected]
                                          ,[PayerExchangeRateExpected]
										from Coinag.PaymentQuoteElements
										where createdDate >= @dateFrom
										--and createdDate < @dateTo");
            payerQuoteQuery.Add("PayersConnectionStrings:Uniteller", @"select [InternalTransactionId] as LedgerId
                                          ,[PayerExchangeRate]
                                          ,[PayerFee]
                                          ,[PayerFeeExpected]
                                          ,[PayerExchangeRateExpected]
										from Uniteller.PaymentQuoteElements
										where createdDate >= @dateFrom
										--and createdDate < @dateTo");



            foreach (var connString in connStrings)
            {
                if (connString.Value != null && payerQuoteQuery.ContainsKey(connString.Key))
                {
                    using (var connection = new SqlConnection(connString.Value))
                    {
                        connection.Open();

                        payersRecords.AddRange(connection.Query<PaymentQuoteElements>(payerQuoteQuery[connString.Key], new { dateFrom, dateTo },null, true, 6000));

                        connection.Close();
                    }
                }

            }
            BulkOperations.UpdateDataByBatch(payersRecords, _configuration.GetConnectionString("MetricsConnString"), "dbo", "TransactionalBase", "LedgerId", 50000);
        }

        public void ModifyPayerExchangeRate(DateTime dateFrom, DateTime dateTo, SpreadSheetConnector conn)
        {
            var peruSheet = conn.ReadDataSheet("Peru", conn.fileLocation);
            var peruData = new List<List<object>>();
            peruData.Add(peruSheet.Select(x => x.First()).ToList());
            peruData.Add(peruSheet.Select(x => x[23]).ToList());
            var peruParsedData = peruData.ParseExchangeRatesVendor("Perú", "PER", "USD", "PEN", "INTERBANK Soles");

            var yapeData = new List<List<object>>();
            yapeData.Add(peruSheet.Select(x => x.First()).ToList());
            yapeData.Add(peruSheet.Select(x => x[11]).ToList());
            var yapeParsedData = yapeData.ParseExchangeRatesVendor("Perú", "PER", "USD", "PEN", "Yape");

            var colombiaSheet = conn.ReadDataSheet("TC Cobru-Cobre", conn.fileLocation, 26);
            var cobruData = new List<List<object>>();
            cobruData.Add(colombiaSheet.Select(x => x.First()).ToList());
            cobruData.Add(colombiaSheet.Select(x => x[11]).ToList());
            var cobruParsedData = cobruData.ParseExchangeRatesVendor("Colombia", "COL", "USD", "COP", "COBRU");

            var cobreData = new List<List<object>>();
            cobreData.Add(colombiaSheet.Select(x => x[14]).ToList());
            cobreData.Add(colombiaSheet.Select(x => x[25]).ToList());
            var cobreParsedData = cobreData.ParseExchangeRatesVendor("Colombia", "COL", "USD", "COP", "COBRE");

            using (var ctx = new RemiteeServicesMetricsContext(_configuration))
            {
                ctx.Database.SetCommandTimeout(900);
                var payerExchangeRates = new List<PayerExchangeRateDTO>();

                List<LedgerPayerExchangeRatesDTO> ledgerPayersExchangeRates = new List<LedgerPayerExchangeRatesDTO>();
                using (var connection = new SqlConnection(_configuration.GetConnectionString("LedgerConnString")))
                {
                    connection.Open();
                    SqlCommand command = new SqlCommand();
                    command.Connection = connection;
                    var queryString = @"SELECT ex.id,
					createdDate,
					validFrom,
					validTo,
					targetToUSD,
					p.name as payerName,
					p.currency as payerCurrency
                  FROM [dbo].[PayerExchangeRates] ex
                  left join Payers p on p.id=ex.payerId
                  where validFrom<@dateTo and validTo>=@dateFrom
					and p.name!='ITALCAMBIO'";
                    ledgerPayersExchangeRates.AddRange(connection.Query<LedgerPayerExchangeRatesDTO>(queryString, new { dateFrom, dateTo }));

                    connection.Close();
                }

                var reducedLedgerPayersExchangeRates = new List<LedgerPayerExchangeRatesDTO>();
                var payersList = ledgerPayersExchangeRates.Select(x => new { x.PayerName, x.PayerCurrency }).Distinct().ToList();

                foreach (var payer in payersList)
                {
                    var orderedExRates = ledgerPayersExchangeRates.Where(x => x.PayerName == payer.PayerName && x.PayerCurrency == payer.PayerCurrency).OrderBy(x => x.CreatedDate);
                    bool finished = false;
                    var currentItem = orderedExRates.FirstOrDefault();

                    while (!finished)
                    {
                        var nextItem = orderedExRates.Where(x => x.TargetToUSD != currentItem.TargetToUSD && x.CreatedDate > currentItem.CreatedDate).OrderBy(x => x.CreatedDate).FirstOrDefault();
                        if (nextItem == null)
                        {
                            reducedLedgerPayersExchangeRates.Add(new LedgerPayerExchangeRatesDTO
                            {
                                CreatedDate = currentItem.CreatedDate,
                                Id = currentItem.Id,
                                TargetToUSD = currentItem.TargetToUSD,
                                ValidFrom = currentItem.CreatedDate > currentItem.ValidFrom ? currentItem.CreatedDate : currentItem.ValidFrom,
                                ValidTo = currentItem.ValidTo,
                                PayerName = currentItem.PayerName == "MAXICAMBIOS" ? "MAXICAMBIO Outbound" : currentItem.PayerName,
                                PayerCurrency = currentItem.PayerCurrency
                            });
                            finished = true;
                        }
                        else
                        {
                            reducedLedgerPayersExchangeRates.Add(new LedgerPayerExchangeRatesDTO
                            {
                                CreatedDate = currentItem.CreatedDate,
                                Id = currentItem.Id,
                                TargetToUSD = currentItem.TargetToUSD,
                                ValidFrom = currentItem.ValidFrom,
                                ValidTo = nextItem.CreatedDate > nextItem.ValidFrom ? nextItem.CreatedDate : nextItem.ValidFrom,
                                PayerName = currentItem.PayerName == "MAXICAMBIOS" ? "MAXICAMBIO Outbound" : currentItem.PayerName,
                                PayerCurrency = currentItem.PayerCurrency
                            });
                            currentItem = nextItem;
                        }
                    }

                }


                foreach (var ledgerPayerExchangeRate in reducedLedgerPayersExchangeRates)
                {
                    payerExchangeRates.AddRange(ctx.TransactionalBases
                        .Where(x => x.SettledAt >= ledgerPayerExchangeRate.ValidFrom
                        && x.SettledAt < ledgerPayerExchangeRate.ValidTo
                        && x.Vendor.ToUpper() == ledgerPayerExchangeRate.PayerName.ToUpper()
                        && x.TargetCurrency == ledgerPayerExchangeRate.PayerCurrency)
                        .Select(x => new PayerExchangeRateDTO
                        {
                            Id = x.Id,
                            PayerExchangeRate = x.PayerExchangeRate ?? ledgerPayerExchangeRate.TargetToUSD
                        }));
                }
                payerExchangeRates.AddRange(ctx.TransactionalBases
                        .Where(x => x.CreatedAt >= dateFrom
                        && x.CreatedAt < dateTo
                        && x.TargetCurrency == "USD")
                        .Select(x => new PayerExchangeRateDTO
                        {
                            Id = x.Id,
                            PayerExchangeRate = 1
                        }).ToList());
                BulkOperations.UpdateData(payerExchangeRates, _configuration.GetConnectionString("MetricsConnString"), "dbo", "TransactionalBase", "Id");

                var vendorExRates = new List<ExchangeRate>();
                var exRates = new List<PayerExchangeRateDTO>();
                for (var dt = dateFrom; dt < dateTo; dt = dt.AddDays(1))
                {
                    vendorExRates.Clear();
                    vendorExRates.Add(peruParsedData.Where(x => x.Date == dt.Date).FirstOrDefault());
                    vendorExRates.Add(yapeParsedData.Where(x => x.Date == dt.Date).FirstOrDefault());
                    vendorExRates.Add(cobruParsedData.Where(x => x.Date == dt.Date).FirstOrDefault());
                    vendorExRates.Add(cobreParsedData.Where(x => x.Date == dt.Date).FirstOrDefault());
                    var currencies = vendorExRates.Select(x => new { x.TargetCurrency, x.Vendor });
                    foreach (var currency in currencies)
                    {
                        var exRateVendor = vendorExRates.Where(x => x.TargetCurrency == currency.TargetCurrency && x.Vendor.ToUpper() == currency.Vendor.ToUpper()).FirstOrDefault()?.ExchangeRate1;

                        //var test = ctx.TransactionalBases
                        //    .Where(x => x.CreatedAt.Date == dt.Date && x.TargetCurrency == currency.TargetCurrency && x.Vendor.ToUpper() == currency.Vendor.ToUpper())
                        //    .Select(x => x.Id).ToList();
                        //foreach (var id in test)
                        //{
                        //    exRates.Add(new PayerExchangeRateDTO
                        //    {
                        //        Id = id,
                        //        PayerExchangeRate = exRateVendor
                        //    });
                        //}
                        var consulta = ctx.TransactionalBases
                            .Where(x => x.CreatedAt.Date == dt.Date && x.TargetCurrency == currency.TargetCurrency && x.Vendor.ToUpper() == currency.Vendor.ToUpper())
                            .Select(x => new PayerExchangeRateDTO
                            {
                                Id = x.Id,
                                PayerExchangeRate = Math.Round(exRateVendor ?? 0M, 8)
                            });
                        exRates.AddRange(ctx.TransactionalBases
                            .Where(x => x.CreatedAt.Date == dt.Date && x.TargetCurrency == currency.TargetCurrency && x.Vendor.ToUpper() == currency.Vendor.ToUpper())
                            .Select(x => new PayerExchangeRateDTO
                            {
                                Id = x.Id,
                                PayerExchangeRate = Math.Round(exRateVendor ?? 0, 8)
                            }));
                    }
                }
                BulkOperations.UpdateData(exRates, _configuration.GetConnectionString("MetricsConnString"), "dbo", "TransactionalBase", "Id");

            }
        }

        public void ModifySpreads(DateTime dateFrom, DateTime dateTo)
        {
            using (SqlConnection conn = new SqlConnection(_configuration.GetConnectionString("MetricsConnString")))
            {
                using (SqlCommand command = new SqlCommand("", conn))
                {
                    conn.Open();
                    command.Parameters.Add("@dateFrom", SqlDbType.Date).Value = dateFrom;
                    command.Parameters.Add("@dateTo", SqlDbType.Date).Value = dateTo;
                    command.CommandTimeout = 600;
                    var updateFxCommandWallet = @"update TransactionalBase 
set NetAmountUSD = case SourceCurrency 
					when 'ARS' then NetAmountSc / ArsexchangeRate --ArsRexchangeRate 
					when 'CLP' then NetAmountSc / ClpexchangeRate --ClpexchangeRate
					else 0
					end,
	FeeAmountUSD = case SourceCurrency 
					when 'ARS' then NetAmountSc * 0.13 / ArsexchangeRate 
					when 'CLP' then NetAmountSc * 0.11 / ClpexchangeRate
					else 0
					end,
	VATUSD = case SourceCurrency 
					when 'ARS' then NetAmountSc * 0.03 * Vatrate / ArsexchangeRate 
					when 'CLP' then 0
					else 0
					end
where Source='Wallet' and CreatedAt >= @dateFrom and CreatedAt < @dateTo";
                    command.CommandText = updateFxCommandWallet;
                    command.ExecuteNonQuery();

                    var updateFxCommandMoneyTransferRemitee = @"update TransactionalBase 
set NetAmountUSD = case TargetCurrency 
					when 'ARS' then case SourceCurrency 
									when 'ARS' then NetAmountSc / ArsexchangeRate --ArsrExchangeRate
									when 'CLP' then NetAmountSc / ExchangeRateSc 
									else NetAmountUSD 
									end
					else NetAmountUSD
					end,
	FeeAmountUSD = case SourceCurrency 
					when 'ARS' then FeeAmountSc / ArsRexchangeRate 
					when 'CLP' then FeeAmountSc / ClpRExchangeRate
					else FeeAmountUSD
					end,
	VATUSD = case SourceCurrency 
					when 'ARS' then Vatrate * FeeAmountSc / ArsRexchangeRate 
					when 'CLP' then Vatrate * FeeAmountSc / ClpRExchangeRate
					else VATUSD
					end,
	SpreadAmountUSD =case SourceCurrency 
					when SourceCurrency='ARS' then case TargetCurrency
									when 'ARS' then 0
									else NetAmountSc / ExchangeRateSc - NetAmountSc / (ExchangeRateSc/(1-SpreadRate))
									end
					when 'CLP' then case TargetCurrency
									when 'ARS' then NetAmountSc / ExchangeRateSc - TargetAmountTc / ArsRexchangeRate
									else NetAmountSc / ExchangeRateSc - NetAmountUsd
									end
					else SpreadAmountUSD
					end
where Source='MoneyTransfer' and Client = 'REMITEE' and CreatedAt >= @dateFrom and CreatedAt < @dateTo";
                    command.CommandText = updateFxCommandMoneyTransferRemitee;
                    command.ExecuteNonQuery();

                    var updateFxCommandMoneyTransferOthers = @"update TransactionalBase 
set SpreadAmountUSD =case  
					when TargetCurrency = 'ARS' then NetAmountUsd - TargetAmountTc / MarketExchangeRate
					when SpreadRate is null then 0
					when TargetCurrency = 'VEF' then NetAmountUsd * SpreadRate
					else NetAmountUsd - TargetAmountTc / MarketExchangeRate
					end,
	SpreadAmountSC =case 
					when TargetCurrency='ARS' then NetAmountUsd - TargetAmountTc / MarketExchangeRate
					when SpreadAmountSC=0 and SourceCurrency='USD' then NetAmountUsd - TargetAmountTc / MarketExchangeRate
					else SpreadAmountSC
					end
where Source='MoneyTransfer' and Client != 'REMITEE' and CreatedAt >= @dateFrom and CreatedAt < @dateTo";
                    command.CommandText = updateFxCommandMoneyTransferOthers;
                    command.ExecuteNonQuery();

                }
            }
        }

        public void ModifyMayoritySpread(int year, int month)
        {
            using (SqlConnection conn = new SqlConnection(_configuration.GetConnectionString("MetricsConnString")))
            {
                using (SqlCommand command = new SqlCommand("", conn))
                {
                    conn.Open();
                    command.CommandTimeout = 600;


                    var createTableCommand = "CREATE TABLE #TmpTable(min decimal(20,8),max decimal(20,8),startDate datetime2(7),endDate datetime2(7), spread decimal(20,8))";
                    command.CommandText = createTableCommand;
                    command.ExecuteNonQuery();

                    var fillTableCommand = $"insert into #TmpTable values" +
                        $"(0, 2000000, datetimefromparts({year.ToString()}, {month.ToString()}, 1,0,0,0,0), null, 0)," +
                        $"(2000000, 5000000, null, null, 0.004)," +
                        $"(5000000, 10000000, null, null, 0.01)," +
                        $"(10000000, 20000000, null, null, 0.014)";
                    command.CommandText = fillTableCommand;
                    command.ExecuteNonQuery();

                    var updateTableCommand = $"update #TmpTable set startDate=(select top 1 x.createdat " +
                        $"from (select tb.createdat," +
                        $"sum(tb.netamountusd) over (order by tb.createdAt asc) as tot " +
                        $"from TransactionalBase tb " +
                        $"where tb.Client like '%mayority%' and tb.TargetCurrency='VEF' and tb.NetAmountUSD<100 " +
                        $"and tb.AccountingPeriod = '{year.ToString() + "-" + month.ToString("00")}') x " +
                        $"where tot>min " +
                        $"order by x.CreatedAt asc), endDate=(select top 1 x.createdat " +
                        $"from (select tb.createdat, " +
                        $"sum(tb.netamountusd) over (order by tb.createdAt asc) as tot " +
                        $"from TransactionalBase tb " +
                        $"where tb.Client like '%mayority%' and tb.TargetCurrency='VEF' and tb.NetAmountUSD<100 " +
                        $"and tb.AccountingPeriod = '{year.ToString() + "-" + month.ToString("00")}') x " +
                        $"where tot>max " +
                        $"order by x.CreatedAt asc);";
                    command.CommandText = updateTableCommand;
                    command.ExecuteNonQuery();

                    var updateTransactionalBaseCommand = $"update T set T.SpreadRate = T.SpreadRate - Temp.spread, " +
                                                        $"T.SpreadAmountUSD = T.SpreadAmountUSD - T.NetAmountUSD * Temp.spread, " +
                                                        $"T.SpreadAmountSC = T.SpreadAmountSC - T.NetAmountSC * Temp.spread " +
                                                        $"FROM TransactionalBase T " +
                                                        $"INNER JOIN #TmpTable Temp ON Temp.startDate<=T.CreatedAt " +
                                                        $"and isnull(Temp.endDate,'{new DateTime(year, month, 1).AddMonths(1).ToString("yyyyMMdd")}')>T.CreatedAt " +
                                                        $"and T.Client like '%mayority%' " +
                                                        $"and (T.TargetCurrency='VEF' or T.TargetCurrency='VES') " +
                                                        $"and T.NetAmountUSD<100; DROP TABLE #TmpTable;";
                    command.CommandText = updateTransactionalBaseCommand;
                    command.ExecuteNonQuery();
                }
            }
        }

        public void AddSpreadDto(DateTime dateFrom, DateTime dateTo, ObPartner obPartner)
        {
            if (obPartner == ObPartner.Italcambio)
            {
                var dailyExRates = new List<ItalcambioExchangeRateDTO>();
                using (var connection = new SqlConnection(_configuration.GetSection("PayersConnectionStrings:Italcambio").Value))
                {
                    connection.Open();
                    var command = @"  SELECT t.TransactionId as LedgerId,
t.CreatedAt,
ROW_NUMBER() OVER(partition by concat(year(t.CreatedAt),'-',month(t.createdAt)) ORDER BY CreatedAt ASC) AS RowNumber,
cast(x.ExchangeRate as decimal(20,8)) as ExchangeRate
  FROM [Italcambio].[ItalcambioCashOutTransactions] t (NOLOCK)
  left join (
	select 
		tx.ExchangeRate/(1-0.03) as ExchangeRate,
		min(tx.createdAt) as ValidFrom,
		max(tx.CreatedAt) as ValidTo
	from [Italcambio].[ItalcambioCashOutTransactions] tx (NOLOCK)
	where isnull(tx.SendAmount/nullif(tx.ExchangeRate,0),0)<100
	and tx.ExchangeRate>1
	group by tx.ExchangeRate/(1-0.03)
  ) x on x.ValidTo >= t.CreatedAt and x.validFrom <= t.CreatedAt
  where CreatedAt >= @dateFrom ";
                    var result = connection.Query<ItalcambioExchangeRateDTO>(command, new { dateFrom = new DateTime(dateFrom.Year, dateFrom.Month, 1), dateTo });
                    dailyExRates.AddRange(result.Where(x => x.CreatedAt >= dateFrom));

                    connection.Close();
                }

                var orderedDailyExRates = dailyExRates.OrderBy(x => x.RowNumber).ToList();
                var lastExRate = orderedDailyExRates.First(x => x.ExchangeRate != null).ExchangeRate;
                for (int i = 0; i < orderedDailyExRates.Count; i++)
                {
                    if (orderedDailyExRates[i].ExchangeRate == null)
                    {
                        orderedDailyExRates[i].ExchangeRate = lastExRate;
                    }
                    else { lastExRate = orderedDailyExRates[i].ExchangeRate; }
                }
                DataTable dt = new DataTable("MyTable");
                dt = orderedDailyExRates.ToDataTable();

                using (SqlConnection conn = new SqlConnection(_configuration.GetConnectionString("MetricsConnString")))
                {

                    using (SqlCommand command = new SqlCommand("", conn))
                    {
                        try
                        {
                            conn.Open();
                            //Creating temp table on database
                            var createTableCommand = "CREATE TABLE #TmpTable(LedgerId int, CreatedAt datetime2, RowNumber int, ExchangeRate decimal(20,8))";
                            command.CommandText = createTableCommand;
                            command.ExecuteNonQuery();

                            //Bulk insert into temp table
                            using (SqlBulkCopy bulkcopy = new SqlBulkCopy(conn))
                            {
                                bulkcopy.BulkCopyTimeout = 660;
                                bulkcopy.DestinationTableName = "#TmpTable";
                                bulkcopy.WriteToServer(dt);
                                bulkcopy.Close();
                            }

                            // Updating destination table, and dropping temp table
                            command.CommandTimeout = 600;
                            var updateTableCommand = "UPDATE T " +
                            "SET " +
                            "T.MarketExchangeRate = temp.ExchangeRate," +
                            "T.PayerExchangeRate = case when temp.RowNumber <= 10000 or T.NetAmountUSD >= 100 then temp.ExchangeRate*0.97 " +
                                "when temp.RowNumber > 10000 and temp.RowNumber <= 15000 then temp.ExchangeRate * (1 - 0.02675) " +
                                "when temp.RowNumber > 15000 and temp.RowNumber <= 20000 then temp.ExchangeRate * (1 - 0.02375) " +
                                "when temp.RowNumber > 20000 then temp.ExchangeRate * (1 - 0.012) end " +
                            "FROM TransactionalBase T " +
                            $"INNER JOIN #TmpTable Temp ON T.LedgerId = temp.LedgerId and T.TargetCurrency!='USD' and T.createdAt between '{dateFrom.ToString("yyyyMMdd")}' and '{dateTo.ToString("yyyyMMdd")}'; ";
                            command.CommandText = updateTableCommand;
                            command.ExecuteNonQuery();

                            var affectedRows = conn.Query<int>("select @@ROWCOUNT;DROP TABLE #TmpTable;").FirstOrDefault();
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex.Message);
                        }
                        finally
                        {
                            conn.Close();
                        }
                    }
                }
            }
        }

        public void AddCalculatedPayerFees(DateTime dateFrom, DateTime dateTo)
        {
            using (SqlConnection conn = new SqlConnection(_configuration.GetConnectionString("MetricsConnString")))
            {
                var feeStrategies = new List<FeeReferences>();
                using (SqlCommand command = new SqlCommand("", conn))
                {
                    conn.Open();
                    feeStrategies.AddRange(conn.Query<FeeReferences>(@"select * from FeeReferences where validFrom <= @dateFrom and (validTo > @dateTo or validTo is null)", new { dateFrom, dateTo }));

                    var strategies = feeStrategies.GroupBy(x => x.FeeStrategy);
                    var fees = new List<PayerFees>();

                    foreach (var strat in strategies)
                    {
                        fees = conn.Query<PayerFees>(@"select * from PayerFees where feeStrategy = @StrategyId order by Priority desc", new { StrategyId = strat.Key }).ToList();
                        var payersList = strat.Select(x => x.PayerRouteCode);
                        var payersListString = string.Join("','", strat.Select(x => x.PayerRouteCode).ToArray());

                        if (fees.First().Type == FeeType.General)
                        {
                            var fee = fees.First();
                            var fix = fee.Fix ?? 0;
                            var min = fee.Min ?? 0;
                            var max = fee.Max ?? 0;
                            var currency = fee.Currency;
                            var percentage = fee.Percentage ?? 0;
                            var updatePayerFeeCommand = $"UPDATE T SET T.ObFeeCurrency = '{currency}', " +
                                $"T.ObFeeAmountTc = LEAST(GREATEST({min.ToString(new CultureInfo("en-US"))},{percentage.ToString(new CultureInfo("en-US"))} " +
                                    $"*(case when '{currency}'='USD' " +
                                    $"then T.Payed " +
                                    $"else Tb.TargetAmountTC end)) " +
                                    $",case when {max.ToString(new CultureInfo("en-US"))} = 0 " +
                                    $"then GREATEST({min.ToString(new CultureInfo("en-US"))},{percentage.ToString(new CultureInfo("en-US"))} " +
                                    $"*(case when '{currency}'='USD' then T.Payed " +
                                    $" else Tb.TargetAmountTC end)) " +
                                    $"else {max.ToString(new CultureInfo("en-US"))} end)+{fix.ToString(new CultureInfo("en-US"))}, " +
                                    $"T.ObFeeAmountUsd = (LEAST(GREATEST({min.ToString(new CultureInfo("en-US"))},{percentage.ToString(new CultureInfo("en-US"))} " +
                                    $"*(case when '{currency}'='USD' " +
                                    $"then T.Payed " +
                                    $"else Tb.TargetAmountTC end)) " +
                                    $",case when {max.ToString(new CultureInfo("en-US"))} = 0 " +
                                    $"then GREATEST({min.ToString(new CultureInfo("en-US"))},{percentage.ToString(new CultureInfo("en-US"))} " +
                                    $"*(case when '{currency}'='USD' then T.Payed " +
                                    $" else Tb.TargetAmountTC end)) " +
                                    $"else {max.ToString(new CultureInfo("en-US"))} end ) + {fix.ToString(new CultureInfo("en-US"))}) / (case when '{currency}' = 'CLP' and Tb.SourceCurrency='CLP' then nullif(Tb.ExchangeRateSC,0) when '{currency}' = 'USD' then 1 else nullif(T.ObExchangeRate,0) end) " + 
                                    $"from TbModifiedFields T " +
                                    $"inner join TransactionalBase Tb on Tb.id=T.TransactionalBaseId " +
                                    $"where Tb.CreatedAt between '{dateFrom.ToString("yyyyMMdd")}' and '{dateTo.ToString("yyyyMMdd")}' " +
                                    $"and Tb.PayerRoute in ('{payersListString}')";
                            command.CommandText = updatePayerFeeCommand;
                            command.ExecuteNonQuery();
                        }

                        if (fees.First().Type == FeeType.TransactionAmountScale)
                        {
                            for (var i = 0; i < fees.Count; i++)
                            {
                                var start = fees[i].Start ?? 0;
                                var end = fees[i].End ?? 0;
                                var currency = fees[i].Currency;
                                var value = fees[i].Fix ?? 0;
                                var updatePayerFeeCommand = $"UPDATE T SET T.ObFeeCurrency = '{currency}', " +
                                    $"T.ObFeeAmountTc = {value.ToString(new CultureInfo("en-US"))}," +
                                    $"T.ObFeeAmountUsd = {value.ToString(new CultureInfo("en-US"))} / (case when '{currency}' = 'CLP' and Tb.SourceCurrency='CLP' then Tb.ExchangeRateSC when '{currency}' = 'USD' then 1 else T.ObExchangeRate end) " +
                                    $"from TbModifiedFields T " +
                                    $"inner join TransactionalBase Tb on Tb.id=T.TransactionalBaseId " +
                                        $"where Tb.CreatedAt between '{dateFrom.ToString("yyyyMMdd")}' and '{dateTo.ToString("yyyyMMdd")}' " +
                                        $"and Tb.PayerRoute in ('{payersListString}') " +
                                        $"and (('{currency}' = 'USD' and Tb.netAmountUSD between {start.ToString(new CultureInfo("en-US"))} and (case {end.ToString(new CultureInfo("en-US"))} when 0 then Tb.netAmountUSD else {end.ToString(new CultureInfo("en-US"))} end)) " +
                                        $"or ('{currency}' != 'USD' and Tb.targetAmountTC between {start.ToString(new CultureInfo("en-US"))} and (case {end.ToString(new CultureInfo("en-US"))} when 0 then Tb.targetAmountTC else {end.ToString(new CultureInfo("en-US"))} end)))";
                                command.CommandText = updatePayerFeeCommand;

                                command.ExecuteNonQuery();
                            }
                        }

                        if (fees.First().Type == FeeType.QuantityScale)
                        {
                            var currency = fees.First().Currency;
                            var orederedFees = fees.OrderBy(x => x.Start ?? 0);
                            var affectedTrx = new List<PayerFeeQuantityScaleDTO>();
                            var affectedTrxCommand = @"  SELECT t.Id as Id,
								t.CreatedAt,
								ROW_NUMBER() OVER(partition by concat(year(t.CreatedAt),'-',month(t.createdAt)) ORDER BY t.CreatedAt ASC) AS RowNumber,
								t.netamountUSD as AmountUsd,
								t.targetAmountTC as AmountTc,
                                case when @currency = 'CLP' and t.SourceCurrency='CLP' then t.ExchangeRateSC when @currency = 'USD' then 1 else Tm.ObExchangeRate end as ObExchangeRate
									FROM TransactionalBase t
                                    LEFT JOIN TbModifiedFields tm on t.Id=tm.TransactionalBaseId
									where t.CreatedAt between @dateFrom and @dateTo
								and t.payerRoute = @payerRoute
								and t.Status!='reversed'";
                            foreach (var payer in payersList)
                            {
                                affectedTrx.AddRange(conn.Query<PayerFeeQuantityScaleDTO>(affectedTrxCommand, new { dateFrom = new DateTime(dateFrom.Year, dateFrom.Month, 1), dateTo, payerRoute = payer, currency = currency }));
                            }

                            var toUpdate = new List<ObFeeDTO>();

                            for (var i = 0; i < fees.Count; i++)
                            {
                                var start = fees[i].Start ?? 0;
                                var end = fees[i].End ?? 0;
                                var min = fees[i].Min ?? 0;
                                var fix = fees[i].Fix ?? 0;
                                var percentage = fees[i].Percentage ?? 0;
                                if (currency == "USD")
                                {
                                    var max = (decimal)(fees[i].Max ?? affectedTrx.Max(x => x.AmountUsd));
                                    toUpdate.AddRange(affectedTrx
                                    .Where(x => x.RowNumber >= start && (end == 0 || x.RowNumber <= end))
                                    .Select(x => new ObFeeDTO
                                    {
                                        TransactionalBaseId = x.Id,
                                        ObFeeCurrency = currency,
                                        ObFeeAmountTc = Math.Max(Math.Min(max, percentage * x.AmountUsd ?? 0), min) + fix,
                                        ObFeeAmountUsd = Math.Max(Math.Min(max, percentage * x.AmountUsd ?? 0), min) + fix
                                    }));

                                }
                                else
                                {
                                    var max = (decimal)(fees[i].Max ?? affectedTrx.Max(x => x.AmountTc));
                                    toUpdate.AddRange(affectedTrx
                                    .Where(x => x.RowNumber >= start && (end == 0 || x.RowNumber <= end))
                                    .Select(x => new ObFeeDTO
                                    {
                                        TransactionalBaseId = x.Id,
                                        ObFeeCurrency = currency,
                                        ObFeeAmountTc = Math.Max(Math.Min(max, percentage * x.AmountTc ?? 0), min) + fix,
                                        ObFeeAmountUsd = (Math.Max(Math.Min(max, percentage * x.AmountTc ?? 0), min) + fix) / x.ObExchangeRate
                                    }));
                                }


                            }
                            BulkOperations.UpdateData(toUpdate, _configuration.GetConnectionString("MetricsConnString"), "dbo", "TbModifiedFields", "TransactionalBaseId");

                        }

                    }

                }
            }
        }

        public void ModifyMissingPayerExchangeRates(DateTime dateFrom, DateTime dateTo)
        {
            using (SqlConnection conn = new SqlConnection(_configuration.GetConnectionString("MetricsConnString")))
            {

                using (SqlCommand command = new SqlCommand("", conn))
                {
                    conn.Open();
                    command.CommandTimeout = 1800;
                    var updateTableCommand = @"update tb set tb.PayerExchangeRate=(select top 1 tx.PayerExchangeRate 
									                    from TransactionalBase tx 
									                    where tx.PayerRoute=tb.PayerRoute 
									                    and tx.PayerExchangeRate is not null 
									                    and tx.PayerExchangeRate!=0
									                    and tx.CreatedAt<tb.CreatedAt
									                    order by tx.CreatedAt desc)
	                                            from TransactionalBase tb
	                                            where (tb.PayerExchangeRate =0 or tb.PayerExchangeRate is null) " +
	                                            $"and tb.AccountingPeriod='{dateFrom.ToString("yyyy-MM")}' " +
                                                $"and tb.TargetCurrency not in ('clp','ars') " +
                                                $"and tb.CollectMethod!='topup'";
                    command.CommandText = updateTableCommand;
                    command.ExecuteNonQuery();

                    updateTableCommand = @"update tb set tb.PayerExchangeRate=(select top 1 tx.PayerExchangeRate 
									                    from TransactionalBase tx 
									                    where tx.TargetCurrency=tb.TargetCurrency 
									                    and tx.PayerExchangeRate is not null 
									                    and tx.PayerExchangeRate!=0
									                    and tx.CreatedAt<tb.CreatedAt
									                    order by tx.CreatedAt desc)
	                                            from TransactionalBase tb
	                                            where (tb.PayerExchangeRate =0 or tb.PayerExchangeRate is null) " +
                                                $"and tb.AccountingPeriod='{dateFrom.ToString("yyyy-MM")}' " +
                                                $"and tb.TargetCurrency not in ('clp','ars') " +
                                                $"and tb.CollectMethod!='topup'";
                    command.CommandText = updateTableCommand;
                    command.ExecuteNonQuery();

                    updateTableCommand = @"update tb set tb.clprExchangeRate=(select top 1 tx.clprExchangeRate 
									                    from TransactionalBase tx 
									                    where tx.clprExchangeRate is not null 
									                    and tx.CreatedAt<tb.CreatedAt
									                    order by tx.CreatedAt desc)
	                                            from TransactionalBase tb
	                                            where tb.clprExchangeRate is null " +
                                                $"and tb.AccountingPeriod='{dateFrom.ToString("yyyy-MM")}' " +
                                                $"and (tb.TargetCurrency = 'clp' or tb.sourceCurrency='clp')" +
                                                $"and tb.CollectMethod!='topup'";
                    command.CommandText = updateTableCommand;
                    command.ExecuteNonQuery();

                    updateTableCommand = @"update tb set tb.ArsrExchangeRate=(select top 1 tx.ArsrExchangeRate 
									                    from TransactionalBase tx 
									                    where tx.arsrExchangeRate is not null 
									                    and tx.CreatedAt<tb.CreatedAt
									                    order by tx.CreatedAt desc)
	                                            from TransactionalBase tb
	                                            where tb.arsrExchangeRate is null " +
                                                $"and tb.AccountingPeriod='{dateFrom.ToString("yyyy-MM")}' " +
                                                $"and (tb.TargetCurrency = 'ars' or tb.sourceCurrency='ars')" +
                                                $"and tb.CollectMethod!='topup'";
                    command.CommandText = updateTableCommand;
                    command.ExecuteNonQuery();
                }
            }

        }

        public void AddModifiedFields(DateTime dateFrom, DateTime dateTo)
        {
            using (SqlConnection conn = new SqlConnection(_configuration.GetConnectionString("MetricsConnString")))
            {
                using (SqlCommand command = new SqlCommand("", conn))
                {
                    conn.Open();
                    command.Parameters.Add("@dateFrom", SqlDbType.Date).Value = dateFrom;
                    command.Parameters.Add("@dateTo", SqlDbType.Date).Value = dateTo;
                    command.CommandTimeout = 600;

                    var updateFxCommandWallet = @"MERGE INTO TbModifiedFields AS target
                    USING (select * from TransactionalBase where [Source]='Wallet' and CreatedAt >= @dateFrom and CreatedAt < @dateTo) AS source
                    ON source.Id = target.TransactionalBaseId AND source.[Source]='Wallet' and source.CreatedAt >= @dateFrom and source.CreatedAt < @dateTo
                    WHEN MATCHED
                    THEN 
                    UPDATE SET target.NetAmountUSD = case source.SourceCurrency 
					    when 'ARS' then source.NetAmountSc / source.ArsexchangeRate --ArsRexchangeRate 
					    when 'CLP' then source.NetAmountSc / source.ClpexchangeRate --ClpexchangeRate
					    else 0
					    end,
	                target.IbFeeAmountUSD = case source.SourceCurrency 
					    when 'ARS' then source.NetAmountSc * 0.13 / source.ArsexchangeRate 
					    when 'CLP' then source.NetAmountSc * 0.11 / source.ClpexchangeRate
					    else 0
					    end,
	                target.IbVatUsd = case source.SourceCurrency 
					    when 'ARS' then source.NetAmountSc * 0.03 * source.Vatrate / source.ArsexchangeRate 
					    when 'CLP' then 0
					    else 0
					    end
                    WHEN NOT MATCHED BY target
                    THEN INSERT (TransactionalBaseId, NetAmountUSD, IbFeeAmountUSD, IbVatUsd)
                    VALUES (source.Id,
                            case source.SourceCurrency 
					            when 'ARS' then source.NetAmountSc / source.ArsexchangeRate --ArsRexchangeRate 
					            when 'CLP' then source.NetAmountSc / source.ClpexchangeRate --ClpexchangeRate
					            else 0
					        end,
                            case source.SourceCurrency 
					            when 'ARS' then source.NetAmountSc * 0.13 / source.ArsexchangeRate 
					            when 'CLP' then source.NetAmountSc * 0.11 / source.ClpexchangeRate
					            else 0
					        end,
                            case source.SourceCurrency 
					            when 'ARS' then source.NetAmountSc * 0.03 * source.Vatrate / source.ArsexchangeRate 
					            when 'CLP' then 0
					            else 0
					        end);";
                    command.CommandText = updateFxCommandWallet;
                    //command.ExecuteNonQuery();

                    var updateFxCommandMoneyTransferRemitee = @"MERGE INTO TbModifiedFields AS target
                    USING (select * from TransactionalBase where CreatedAt >= @dateFrom and CreatedAt < @dateTo) AS source
                    ON source.Id = target.TransactionalBaseId
                    WHEN MATCHED
                    THEN 
                    UPDATE SET target.NetAmountUSD = case 
                                when source.Client = 'REMITEE' 
                                    then case  
                                    when source.CollectMethod='TopUp' and source.SourceCurrency='ARS' then source.NetAmountSc / nullif(source.ArsexchangeRate,0)
                                    when source.CollectMethod='TopUp' and source.SourceCurrency='CLP' then source.NetAmountSc / nullif(source.ClpexchangeRate,0)
					                when source.TargetCurrency='ARS' then case source.SourceCurrency 
								        when 'ARS' then source.NetAmountSc / nullif(source.ArsRexchangeRate,0)  --ArsrExchangeRate 
								        when 'CLP' then source.NetAmountSc / nullif(source.ExchangeRateSc ,0) 
								        else source.NetAmountUSD 
							            end
                                    else source.NetAmountUSD
                                    end
                                else source.NetAmountUSD
                                end,
                    target.IbSpreadAmountSC = case 
                            when source.targetCurrency != 'USD' and source.PayerRoute like '%italcambio%'
                            then source.SpreadAmountSC + 0.03 * source.NetAmountSc
                            when source.TransactionType = 3 then source.NetAmountUsd - source.TargetAmountTC / nullif(source.MarketExchangeRate,0)
                            else source.SpreadAmountSC
                            end,
                    target.IbSpreadAmountUsd = isnull(source.SpreadRate,0) * source.NetAmountUSD,
	                target.IbFeeAmountUSD = case 
                            when source.Client = 'REMITEE' 
                            then 
                                    case source.SourceCurrency 
					                when 'ARS' then source.FeeAmountSc / nullif(source.ArsRexchangeRate,0) 
					                when 'CLP' then source.FeeAmountSc / nullif(source.ClpRExchangeRate,0) 
					                else source.FeeAmountUSD
					                end
                            else source.FeeAmountUSD
                            end,
	                target.IbVatUsd = case 
                            when source.Client = 'REMITEE'
                                then case source.SourceCurrency 
					            when 'ARS' then source.Vatrate * source.FeeAmountSc / nullif(source.ArsRexchangeRate,0) 
					            when 'CLP' then source.Vatrate * source.FeeAmountSc / nullif(source.ClpRExchangeRate,0) 
					            else source.VATUSD
					            end
                            else source.VatUsd
                            end,  
                    target.ObExchangeRate = case source.TargetCurrency
	                          when 'USD'
	                          then 1
	                          when 'ARS'
	                          then source.ARSRExchangeRate
	                          when 'CLP'
	                          then source.CLPRExchangeRate
	                          else source.PayerExchangeRate
	                          end,
                    target.ObExchangeRateAccounting = case source.TargetCurrency
	                          when 'USD'
	                          then 1
	                          when 'ARS'
	                          then source.ARSRExchangeRate
	                          when 'CLP'
	                          then source.CLPRExchangeRate
                              when 'VEF' 
                              then source.ExchangeRateTC
	                          else source.PayerExchangeRate
	                          end,
                    target.ObMarketExchangeRate = case source.TargetCurrency
	                          when 'USD'
	                          then 1
	                          else source.MarketExchangeRate
	                          end
                    WHEN NOT MATCHED BY target
                    THEN INSERT (TransactionalBaseId, NetAmountUSD, IbSpreadAmountSC, IbSpreadAmountUsd, IbFeeAmountUSD, IbVatUsd, ObExchangeRate, ObExchangeRateAccounting, ObMarketExchangeRate)
                    VALUES (source.Id,
                            case 
                                when source.Client = 'REMITEE' 
                                    then case  
                                    when source.CollectMethod='TopUp' and source.SourceCurrency='ARS' then source.NetAmountSc / nullif(source.ArsexchangeRate,0)
                                    when source.CollectMethod='TopUp' and source.SourceCurrency='CLP' then source.NetAmountSc / nullif(source.ClpexchangeRate,0)
					                when source.TargetCurrency='ARS' then case source.SourceCurrency 
								        when 'ARS' then source.NetAmountSc / nullif(source.ArsRexchangeRate,0)  --ArsrExchangeRate 
								        when 'CLP' then source.NetAmountSc / nullif(source.ExchangeRateSc ,0) 
								        else source.NetAmountUSD 
							            end
                                    else source.NetAmountUSD
                                    end
                                else source.NetAmountUSD
                                end,
                            case 
                                when source.targetCurrency != 'USD' and source.PayerRoute like '%italcambio%'
                                then source.SpreadAmountSC + 0.03 * source.NetAmountSc
                                when source.TransactionType = 3 then source.NetAmountUsd - source.TargetAmountTC / nullif(source.MarketExchangeRate,0)
                                else source.SpreadAmountSC
                                end,
                            isnull(source.SpreadRate,0) * source.NetAmountUSD,
                            case 
                            when source.Client = 'REMITEE' 
                                then case source.SourceCurrency 
					            when 'ARS' then source.FeeAmountSc / nullif(source.ArsRexchangeRate ,0)
					            when 'CLP' then source.FeeAmountSc / nullif(source.ClpRExchangeRate,0)
					            else source.FeeAmountUSD
					            end
                            else source.FeeAmountUSD
                            end,
                            case 
                            when source.Client = 'REMITEE'
                                then case source.SourceCurrency 
					            when 'ARS' then source.Vatrate * source.FeeAmountSc / nullif(source.ArsRexchangeRate,0)
					            when 'CLP' then source.Vatrate * source.FeeAmountSc / nullif(source.ClpRExchangeRate,0)
					            else source.VATUSD
					            end
                            else source.VatUsd
                            end,
                            case source.TargetCurrency
	                          when 'USD'
	                          then 1
	                          when 'ARS'
	                          then source.ARSRExchangeRate
	                          when 'CLP'
	                          then source.CLPRExchangeRate
	                          else source.PayerExchangeRate
	                          end,
                            case source.TargetCurrency
	                          when 'USD'
	                          then 1
	                          when 'ARS'
	                          then source.ARSRExchangeRate
	                          when 'CLP'
	                          then source.CLPRExchangeRate
                              when 'VEF' 
                              then source.ExchangeRateTC
	                          else source.PayerExchangeRate
	                          end,
                            case source.TargetCurrency
	                          when 'USD'
	                          then 1
	                          else source.MarketExchangeRate
	                          end);";                        
                    command.CommandText = updateFxCommandMoneyTransferRemitee;
                    command.ExecuteNonQuery();
                }
            }
        }

        public void CalculateModifiedFields(DateTime dateFrom, DateTime dateTo)
        {
            //ITALCAMBIO
            var dailyExRates = new List<ItalcambioExchangeRateDTO>();
            
            using (var connection = new SqlConnection(_configuration.GetSection("PayersConnectionStrings:Italcambio").Value))
            {
                connection.Open();
                var command = @"  SELECT t.TransactionId as LedgerId,
t.CreatedAt,
ROW_NUMBER() OVER(partition by concat(year(t.CreatedAt),'-',month(t.createdAt)) ORDER BY CreatedAt ASC) AS RowNumber,
cast(x.ExchangeRate as decimal(20,8)) as ExchangeRate
  FROM [Italcambio].[ItalcambioCashOutTransactions] t (NOLOCK)
  left join (
	select 
		tx.ExchangeRate/(1-0.03) as ExchangeRate,
		min(tx.createdAt) as ValidFrom,
		max(tx.CreatedAt) as ValidTo
	from [Italcambio].[ItalcambioCashOutTransactions] tx (NOLOCK)
	where isnull(tx.SendAmount/nullif(tx.ExchangeRate,0),0)<100
	and tx.ExchangeRate>1
	group by tx.ExchangeRate/(1-0.03)
  ) x on x.ValidTo >= t.CreatedAt and x.validFrom <= t.CreatedAt
  where CreatedAt >= @dateFrom ";
                var result = connection.Query<ItalcambioExchangeRateDTO>(command, new { dateFrom = new DateTime(dateFrom.Year, dateFrom.Month, 1), dateTo });
                dailyExRates.AddRange(result.Where(x => x.CreatedAt >= dateFrom));

                connection.Close();
            }

            var orderedDailyExRates = dailyExRates.OrderBy(x => x.RowNumber).ToList();
            var lastExRate = orderedDailyExRates.First(x => x.ExchangeRate != null).ExchangeRate;
            for (int i = 0; i < orderedDailyExRates.Count; i++)
            {
                if (orderedDailyExRates[i].ExchangeRate == null)
                {
                    orderedDailyExRates[i].ExchangeRate = lastExRate;
                }
                else { lastExRate = orderedDailyExRates[i].ExchangeRate; }
            }
            DataTable dt = new DataTable("MyTable");
            dt = orderedDailyExRates.ToDataTable();
            
            //Escribimos Metrics
            using (SqlConnection conn = new SqlConnection(_configuration.GetConnectionString("MetricsConnString")))
            {
                using (SqlCommand command = new SqlCommand("", conn))
                {
                    conn.Open();
                    command.CommandTimeout = 600;
                    

                    var createTableCommand = "CREATE TABLE #MayorityTable(min decimal(20,8),max decimal(20,8),startDate datetime2(7),endDate datetime2(7), spread decimal(20,8))";
                    command.CommandText = createTableCommand;
                    command.ExecuteNonQuery();

                    var fillTableCommand = $"insert into #MayorityTable values" +
                        $"(0, 2000000, datetimefromparts({dateFrom.Year}, {dateFrom.Month}, 1,0,0,0,0), null, 0)," +
                        $"(2000000, 5000000, null, null, 0.004)," +
                        $"(5000000, 10000000, null, null, 0.01)," +
                        $"(10000000, 20000000, null, null, 0.014)";
                    command.CommandText = fillTableCommand;
                    command.ExecuteNonQuery();

                    var updateTableCommand = $"create table #Amounts (CreatedAt datetime2(7),Amount decimal(20,8)); " +
                        $"insert into #Amounts " +
                        $"select tb.createdat, " +
                        $"sum(tb.netamountusd) over(order by tb.createdAt asc) as tot " +
                        $"from TransactionalBase tb " +
                        $"where tb.Client in ('mayority', 'mayority-usa-moneytransfer-inbound') " +
                        $"and tb.TargetCurrency = 'VEF' and tb.NetAmountUSD < 100 " +
                        $"and tb.AccountingPeriod = '{dateFrom.Year.ToString() + "-" + dateFrom.Month.ToString("00")}';  " +
                        $"update #MayorityTable set startDate=(select top 1 x.createdat " +
                        $"from #Amounts x " +
                        $"where x.Amount>min " +
                        $"order by x.CreatedAt asc), endDate=isnull((select top 1 x.createdat " +
                        $"from #Amounts x " +
                        $"where x.Amount>max " +
                        $"order by x.CreatedAt asc),DATEFROMPARTS({dateTo.Year},{dateTo.Month},1)); " +
                        $"DROP TABLE #Amounts;";
                    command.CommandText = updateTableCommand;
                    command.ExecuteNonQuery();
                    
                    createTableCommand = "CREATE TABLE #ItalcambioTable(LedgerId int, CreatedAt datetime2, RowNumber int, ExchangeRate decimal(20,8))";
                    command.CommandText = createTableCommand;
                    command.ExecuteNonQuery();

                    //Bulk insert into temp table
                    using (SqlBulkCopy bulkcopy = new SqlBulkCopy(conn))
                    {
                        bulkcopy.BulkCopyTimeout = 660;
                        bulkcopy.DestinationTableName = "#ItalcambioTable";
                        bulkcopy.WriteToServer(dt);
                        bulkcopy.Close();
                    }

                    // Updating destination table, and dropping temp table
                    command.CommandTimeout = 600;
                    updateTableCommand = "UPDATE T " +
                    "SET " +
                    "T.ObMarketExchangeRate = temp.ExchangeRate," +
                    "T.ObExchangeRate = case when temp.RowNumber <= 10000 or T.NetAmountUSD >= 100 then temp.ExchangeRate*0.97 " +
                        "when temp.RowNumber > 10000 and temp.RowNumber <= 15000 then temp.ExchangeRate * (1 - 0.02675) " +
                        "when temp.RowNumber > 15000 and temp.RowNumber <= 20000 then temp.ExchangeRate * (1 - 0.02375) " +
                        "when temp.RowNumber > 20000 then temp.ExchangeRate * (1 - 0.012) end," +
                    "T.ObFeeAmountUSD = case when temp.RowNumber <= 10000 or T.NetAmountUSD >= 100 then 1.25 " +
                        "when temp.RowNumber > 10000 and temp.RowNumber <= 15000 then 0.92 " +
                        "when temp.RowNumber > 15000 and temp.RowNumber <= 20000 then 0.725 " +
                        "when temp.RowNumber > 20000 then 0.475 end," +
                    "T.ObSpreadAmountTC = 0.03 * T.NetAmountUSD," +
                    "T.ObExchangeRateAccounting = temp.ExchangeRate * 0.97, " +
                    "T.ObFeeAmountTC = 1.25, " +
                    "T.ObFeeCurrency = 'USD' " +
                    "FROM TbModifiedFields T " +
                    "INNER JOIN TransactionalBase Tb on Tb.Id = T.TransactionalBaseId " +
                    $"INNER JOIN #ItalcambioTable Temp ON Tb.LedgerId = temp.LedgerId and Tb.TargetCurrency!='USD' and Tb.createdAt between '{dateFrom.ToString("yyyyMMdd")}' and '{dateTo.ToString("yyyyMMdd")}'; ";

                    command.CommandText = updateTableCommand;
                    command.ExecuteNonQuery();
                    
                    var affectedRows = conn.Query<int>("select @@ROWCOUNT;DROP TABLE #ItalcambioTable; ").FirstOrDefault();
                    
                    command.CommandTimeout = 600;
                    updateTableCommand = "UPDATE T " +
                    "SET " +
                    "T.IbSpreadAmountUSD = T.NetAmountUSD * (0.03 - isnull(mt.spread,0)) " +
                    "FROM TbModifiedFields T " +
                    "INNER JOIN TransactionalBase Tb on Tb.Id = T.TransactionalBaseId " +
                    $"LEFT JOIN #MayorityTable mt ON Tb.CreatedAt between mt.startDate and mt.endDate " +
                    $"where Tb.NetAmountUSD<100 " +
                    $"and Tb.Client in ('mayority','mayority-usa-moneytransfer-inbound') " +
                    $"and Tb.TargetCurrency='VEF'" +
                    $"and Tb.AccountingPeriod = '{dateFrom.Year.ToString() + "-" + dateFrom.Month.ToString("00")}' ; "
                    ;
                    command.CommandText = updateTableCommand;
                    command.ExecuteNonQuery();

                    affectedRows = conn.Query<int>("select @@ROWCOUNT; DROP TABLE #MayorityTable;").FirstOrDefault();

                    command.Parameters.Add("@dateFrom", SqlDbType.Date).Value = dateFrom;
                    command.Parameters.Add("@dateTo", SqlDbType.Date).Value = dateTo;
                    command.CommandTimeout = 600;

                    var updateFxCommandMoneyTransferRemitee = @"UPDATE T SET
                    T.IbSpreadAmountUsd = case
	                    when tb.transactionType=1 and tb.targetcurrency != 'VES' then T.NetAmountUsd - tb.TargetAmountTc / nullif(T.ObMarketExchangeRate,0)
		                when tb.transactionType=1 and tb.targetcurrency = 'VES' then T.IbSpreadAmountUSD
		                when tb.transactionType=2 then T.NetAmountUsd - tb.TargetAmountTc / nullif(T.ObMarketExchangeRate, 0)
		                when tb.TransactionType=3 then (tb.TargetAmountTc / nullif(T.ObMarketExchangeRate,0) - TargetAmountTc / nullif(T.ObExchangeRate,0))
		                when tb.TransactionType=4 then T.NetAmountUsd - tb.TargetAmountTc / nullif(T.ObMarketExchangeRate,0)
		                when tb.TransactionType=10 then 0 --TargetAmountTc / nullif(ArsRExchangeRate,0) - TargetAmountTc / nullif(MarketExchangeRate,0)
		                when tb.TransactionType in (11,12) then 0
		                when tb.TransactionType=13 then 0 --NetAmountSc / ExchangeRateSc - NetAmountSc / (ExchangeRateSc/(1-SpreadRate))
		                when tb.TransactionType=14 then 0 --NetAmountSc / ExchangeRateSc - NetAmountSc / (ExchangeRateSc/(1-SpreadRate))
		                when tb.TransactionType=15 then 0 --NetAmountSc / ExchangeRateSc - NetAmountUsd
		                else 0
		                end,
                    T.IbSpreadAmountSC = case 
                        when tb.transactionType=1 and tb.targetcurrency != 'VES' and tb.spreadAmountSC = 0 
                        then T.NetAmountUsd - tb.TargetAmountTc / nullif(T.ObMarketExchangeRate,0) else T.IbSpreadAmountSC end, 
                    T.ObSpreadAmountTc = case tb.transactionType
	                  when 1 then tb.TargetAmountTc / nullif(T.ObExchangeRateAccounting,0) - TargetAmountTc / nullif(T.ObMarketExchangeRate,0)
	                  when 2 then 0
	                  when 3 then tb.TargetAmountTc / nullif(T.ObExchangeRateAccounting,0) - T.NetAmountUsd 
	                  when 4 then tb.TargetAmountTc / nullif(T.ObExchangeRateAccounting,0) - tb.TargetAmountTc / nullif(T.ObMarketExchangeRate,0)
	                  when 10 then 0 --NetAmountSc / ExchangeRateSc - TargetAmountTc / nullif(T.ObExchangeRateAccounting,0)
	                  when 13 then 0 --TargetAmountTc / nullif(T.ObExchangeRateAccounting,0) - TargetAmountTc / nullif(MarketExchangeRate,0)
	                  when 11 then 0
	                  when 12 then 0
	                  when 14 then 0 --TargetAmountTc / nullif(PayerExchangeRate,0) - TargetAmountTc / nullif(MarketExchangeRate,0)
	                  when 15 then 0 --TargetAmountTc / nullif(PayerExchangeRate,0) - TargetAmountTc / nullif(MarketExchangeRate,0)
	                  else 0
	                  end,
	                T.ObSpreadAmountUsd = case tb.transactionType
	                  when 1 then tb.TargetAmountTc / nullif(T.ObExchangeRate,0) - tb.TargetAmountTc / nullif(T.ObMarketExchangeRate,0)
	                  when 2 then 0
	                  when 3 then (tb.TargetAmountTc / nullif(T.ObMarketExchangeRate,0) - T.NetAmountUsd ) 
	                  when 4 then tb.TargetAmountTc / nullif(T.ObExchangeRate,0) - tb.TargetAmountTc / nullif(T.ObMarketExchangeRate,0)
	                  when 10 then 0 --NetAmountSc / ExchangeRateSc - TargetAmountTc / ArsRexchangeRate
	                  when 13 then 0 --TargetAmountTc / nullif(ClpRExchangeRate,0) - TargetAmountTc / nullif(MarketExchangeRate,0)
	                  when 11 then 0
	                  when 12 then 0
	                  when 14 then 0 --TargetAmountTc / nullif(PayerExchangeRate,0) - TargetAmountTc / nullif(MarketExchangeRate,0)
	                  when 15 then 0 --TargetAmountTc / nullif(PayerExchangeRate,0) - TargetAmountTc / nullif(MarketExchangeRate,0)
	                  else 0
	                  end,
                    T.Payed = tb.TargetAmountTC / nullif(T.ObExchangeRateAccounting, 0)
                    FROM TbModifiedFields T
                    INNER JOIN TransactionalBase tb ON tb.Id = T.TransactionalBaseId AND tb.[Source]='MoneyTransfer' and tb.CreatedAt >= @dateFrom and tb.CreatedAt < @dateTo
                    ";
                    command.CommandText = updateFxCommandMoneyTransferRemitee;
                    command.ExecuteNonQuery();
                }
            }
            
            var saasIds = new List<WalletClientDTO>();
            using (var connection = new SqlConnection(_configuration.GetSection("ConnectionStrings:WalletConnString").Value))
            {
                connection.Open();
                var command = @"  select ot.PaymentTransactionId as LedgerId,
concat(u.companyid,'_SaaS') as Client
from wallet.Operations o
left join wallet.ShoppingCarts sc on sc.id=o.ShoppingCartId
left join wallet.users u on u.id=sc.UserId
left join wallet.OperationTransactions ot on ot.OperationId=o.id
where u.CompanyId!='DEFAULT'
and ot.PaymentTransactionId is not null
and o.CreatedDateUTC between dateadd(day,-1,@dateFrom) and dateadd(day,1,@dateTo) ";
                var result = connection.Query<WalletClientDTO>(command, new { dateFrom = dateFrom, dateTo });
                saasIds.AddRange(result);

                connection.Close();
            }
            
            BulkOperations.UpdateData(saasIds, _configuration.GetConnectionString("MetricsConnString"), "dbo", "TransactionalBase", "LedgerId");
            
        }

        public enum ObPartner
        {
            Italcambio
        }

    }
}

