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
    public class MetricsService
    {
        private readonly IConfigurationRoot _configuration;
        public MetricsService(IConfigurationRoot configuration)
        {
            _configuration = configuration;
        }








        public void UpdateTCTables(DateTime dateFrom, DateTime dateTo)
        {
            //UpdateTCCountries();

            UpdateTCSenders(dateFrom, dateTo);

            UpdateTCReceivers(dateFrom, dateTo);

            UpdateTCTransactions(dateFrom, dateTo);
        }



        private void UpdateTCReceivers(DateTime dateFrom, DateTime dateTo)
        {
            var config = new MapperConfiguration(cfg => cfg.CreateMap<ModelsTC.Receiver, Tcreceiver>());

            var mapper = config.CreateMapper();

            var users = new List<ModelsTC.Receiver>();

            using (var ctx = new RemiteeServicesTransactionCollectorDbContext(_configuration))
            {
                users = ctx.Receivers.Where(x => ctx.Transactions.Where(y => y.DateCreated >= dateFrom && y.DateCreated < dateTo && y.ReceiverId == x.AccountId).FirstOrDefault() != null).ToList();
            }

            var toUpdate = mapper.Map<List<ModelsTC.Receiver>, List<Tcreceiver>>(users);

            BulkOperations.UpsertData(toUpdate, _configuration.GetConnectionString("MetricsConnString"), "tc", "Tcreceivers", "Id");

        }



        private void UpdateTCSenders(DateTime dateFrom, DateTime dateTo)
        {
            var config = new MapperConfiguration(cfg => cfg.CreateMap<ModelsTC.Sender, Tcsender>());

            var mapper = config.CreateMapper();

            var users = new List<ModelsTC.Sender>();

            using (var ctx = new RemiteeServicesTransactionCollectorDbContext(_configuration))
            {
                users = ctx.Senders.Where(x => ctx.Transactions.Where(y => y.DateCreated >= dateFrom && y.DateCreated < dateTo && y.SenderId == x.AccountId).FirstOrDefault() != null).ToList();
            }

            var toUpdate = mapper.Map<List<ModelsTC.Sender>, List<Tcsender>>(users);
            BulkOperations.UpsertData(toUpdate, _configuration.GetConnectionString("MetricsConnString"), "tc", "Tcsenders", "Id");
        }





        public void UpdateTCTransactions(DateTime dateFrom, DateTime dateTo)
        {
            var config = new MapperConfiguration(cfg => cfg.CreateMap<Transaction, Tctransaction>());

            var mapper = config.CreateMapper();

            var users = new List<Transaction>();

            using (var ctx = new RemiteeServicesTransactionCollectorDbContext(_configuration))
            {
                users = ctx.Transactions.Where(x => x.DateCreated >= dateFrom && x.DateCreated < dateTo).ToList();
            }

            var toUpdate = mapper.Map<List<Transaction>, List<Tctransaction>>(users);
            BulkOperations.UpsertData(toUpdate, _configuration.GetConnectionString("MetricsConnString"), "tc", "Tctransactions", "Id");

        }
        public void SendRevenue(int year, int month)
        {
            var revenue = new List<TransactionalBase>();
            using (var ctx = new RemiteeServicesMetricsContext(_configuration))
            {
                revenue = ctx.TransactionalBases.Where(x => x.CreatedAt.Year == year && x.CreatedAt.Month == month).ToList();

            }

            SaveToCsv(revenue, @"C:\Users\FrancoZeppilli\Documents\Remitee\Innovacion\Metrics\Revenue - " + month.ToString().PadLeft(2, '0') + year.ToString() + " - decimalSeparatorDot.csv", new CultureInfo("en-US", false));
            SaveToCsv(revenue, @"C:\Users\FrancoZeppilli\Documents\Remitee\Innovacion\Metrics\Revenue - " + month.ToString().PadLeft(2, '0') + year.ToString() + " - decimalSeparatorComma.csv", new CultureInfo("es-ES", false));
            try
            {
                MailMessage messageA = new MailMessage();
                MailMessage message = new MailMessage();
                SmtpClient smtp = new SmtpClient();
                message.From = new MailAddress("franco@remitee.com");
                messageA.From = new MailAddress("franco@remitee.com");

                message.To.Add(new MailAddress("franco@remitee.com"));
                messageA.To.Add(new MailAddress("franco@remitee.com"));

                message.Subject = "Revenue";
                messageA.Subject = "Revenue";
                //message.IsBodyHtml = true; //to make message body as html  
                message.Body = "Envio Revenue con decimales separados por coma";
                messageA.Body = "Envio Revenue con decimales separados por punto";

                messageA.Attachments.Add(new Attachment(@"C:\Users\FrancoZeppilli\Documents\Remitee\Innovacion\Metrics\Revenue - " + month.ToString().PadLeft(2, '0') + year.ToString() + " - decimalSeparatorDot.csv"));
                message.Attachments.Add(new Attachment(@"C:\Users\FrancoZeppilli\Documents\Remitee\Innovacion\Metrics\Revenue - " + month.ToString().PadLeft(2, '0') + year.ToString() + " - decimalSeparatorComma.csv"));
                smtp.Port = 587;
                smtp.Host = "smtp.gmail.com"; //for gmail host  
                smtp.EnableSsl = true;
                smtp.UseDefaultCredentials = false;
                smtp.Credentials = new NetworkCredential("franco@remitee.com", "rzrbhgunvkhyydkw");
                smtp.DeliveryMethod = SmtpDeliveryMethod.Network;
                smtp.Timeout = 6000000;
                smtp.Send(message);
                smtp.Send(messageA);
            }
            catch (Exception ex) { Console.WriteLine(ex); };
        }

        public static void SaveToCsv<T>(List<T> reportData, string path, CultureInfo culture)
        {
            CultureInfo.CurrentCulture = culture;
            var lines = new List<string>();
            IEnumerable<PropertyDescriptor> props = TypeDescriptor.GetProperties(typeof(T)).OfType<PropertyDescriptor>();
            var header = string.Join(";", props.ToList().Select(x => x.Name));
            lines.Add(header);
            var valueLines = reportData.Select(row => string.Join(";", header.Split(';').Select(a => row.GetType().GetProperty(a).GetValue(row, null))));
            lines.AddRange(valueLines);
            File.WriteAllLines(path, lines.ToArray(), Encoding.UTF8);

        }






        public void AddArsExchangeRateOf(DateTime dateFrom, DateTime dateTo)
        {
            using (var ctx = new RemiteeServicesMetricsContext(_configuration))
            {
                var toUpdate = ctx.TransactionalBases.Where(x => x.CreatedAt >= dateFrom && x.CreatedAt < dateTo).ToList();
                var arsOfExRates = new List<ExchangeRate>();
                arsOfExRates = ctx.ExchangeRates.Where(x => x.Date >= dateFrom
                && x.Date <= dateTo
                && x.TargetCurrency == "ARS").ToList();

                foreach (var item in toUpdate)
                {
                    item.ArsExchangeRateOf = arsOfExRates.Where(x => x.Date == item.CreatedAt.Date).FirstOrDefault().ExchangeRate1;
                }
                ctx.SaveChanges();

            }
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



            foreach (var connString in connStrings)
            {
                if (connString.Value != null && payerQuoteQuery.ContainsKey(connString.Key))
                {
                    using (var connection = new SqlConnection(connString.Value))
                    {
                        connection.Open();
                        SqlCommand command = new SqlCommand();
                        command.Connection = connection;
                        command.CommandText = payerQuoteQuery[connString.Key];
                        command.Parameters.Add("@dateFrom", SqlDbType.Date).Value = dateFrom;
                        command.Parameters.Add("@dateTo", SqlDbType.Date).Value = dateTo;
                        payersRecords.AddRange(connection.Query<PaymentQuoteElements>(payerQuoteQuery[connString.Key], new { dateFrom, dateTo }));

                        connection.Close();
                    }
                }

            }
            BulkOperations.UpdateData(payersRecords, _configuration.GetConnectionString("MetricsConnString"), "dbo", "TransactionalBase", "LedgerId");
        }
    }


}
