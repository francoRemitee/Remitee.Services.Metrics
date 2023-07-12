using Dapper;
using Microsoft.Extensions.Configuration;
using Remitee.Services.Metrics.Models;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Remitee.Services.Metrics.Extensions
{
    public static class DataExtensions
    {
        public static List<TransactionalBase> AddPayersData(this List<TransactionalBase> list, DateTime dateFrom, DateTime dateTo, IConfigurationRoot config)
        {
            List<PaymentQuoteElements> payersRecords = new List<PaymentQuoteElements>();
            var connStrings = config.GetRequiredSection("PayersConnectionStrings").AsEnumerable();
            Dictionary<string, string> payerQuoteQuery = new Dictionary<string, string>();
            payerQuoteQuery.Add("PayersConnectionStrings:DLocal", @"select *
										from dlocal.PaymentQuoteElements
										where createdDate >= @dateFrom
										and createdDate < @dateTo");
            payerQuoteQuery.Add("PayersConnectionStrings:EasyPagos", @"select *
										from Easypagos.PaymentQuoteElements
										where createdDate >= @dateFrom
										and createdDate < @dateTo");
            payerQuoteQuery.Add("PayersConnectionStrings:Italcambio", @"select i.Id,
                                        i.TransactionId as InternalTransactionId,
                                        i.ExchangeRate as PayerExchangeRate,
                                        p.PayerFee,
                                        f.BaseFee as PayerFeeExpected,
                                        i.CreatedAt as CreatedDate
                                        from Italcambio.ItalcambioCashOutTransactions i
                                        left join Italcambio.PaymentQuoteElements p on p.InternalTransactionId=i.TransactionId
                                        left join Italcambio.PayerFees f on f.Currency=i.ReceiveCurrency and i.CreatedAt>=f.ValidFrom and i.CreatedAt<=isnull(f.ValidTo,@dateTo)
                                        where i.createdAt >= @dateFrom
                                        and i.createdAt < @dateTo");
            payerQuoteQuery.Add("PayersConnectionStrings:LocalPayment", @"select *
										from LocalPayment.PaymentQuoteElements
										where createdDate >= @dateFrom
										and createdDate < @dateTo");
            payerQuoteQuery.Add("PayersConnectionStrings:Maxicambios", @"select *
										from messaging.PaymentQuoteElements
										where createdDate >= @dateFrom
										and createdDate < @dateTo");
            payerQuoteQuery.Add("PayersConnectionStrings:Radar", @"select *
										from Radar.PaymentQuoteElements
										where createdDate >= @dateFrom
										and createdDate < @dateTo");
            payerQuoteQuery.Add("PayersConnectionStrings:RippleNetCloud", @"select *
										from messaging.PaymentQuoteElements
										where createdDate >= @dateFrom
										and createdDate < @dateTo");
            payerQuoteQuery.Add("PayersConnectionStrings:TransferZero", @"select *
										from TransferZero.PaymentQuoteElements
										where createdDate >= @dateFrom
										and createdDate < @dateTo");

            foreach (var connString in connStrings)
            {
                if(connString.Value != null)
                {
                    using (var connection = new SqlConnection(connString.Value))
                    {
                        connection.Open();
                        SqlCommand command = new SqlCommand();
                        command.Connection = connection;
                        command.CommandText = payerQuoteQuery[connString.Key];
                        command.Parameters.Add("@dateFrom", SqlDbType.Date).Value = dateFrom;
                        command.Parameters.Add("@dateTo", SqlDbType.Date).Value = dateTo;
                        payersRecords.AddRange(connection.Query<PaymentQuoteElements>(payerQuoteQuery[connString.Key], new { dateFrom = dateFrom, dateTo = dateTo }));

                        connection.Close();
                    }
                }
               
            }
            foreach (var trx in list)
            {
                var payerData = payersRecords.Where(y => y.InternalTransactionId == trx.LedgerId).FirstOrDefault();
                trx.PayerExchangeRate = payerData?.PayerExchangeRate ?? null;
                trx.RemiteeCalculatedFee = payerData?.RemiteeCalculatedFee ?? null;
                trx.PayerFee = payerData?.PayerFee ?? null;
                trx.PayerFeeExpected = payerData?.PayerFeeExpected ?? null;
                trx.PayerExchangeRateExpected = payerData?.PayerExchangeRateExpected ?? null;

            }

            return list;
        }
        
        public static List<IList<object>> CreateListOfLists<T>(this IEnumerable<T> entities, bool withHeaders)
        {
            var dt = new List<IList<object>>();
            var x = typeof(T).GetProperties().Select(x => x.Name.Cast<object>());
            //creating headers
            if( withHeaders)
            {
                dt.Add(typeof(T).GetProperties().Select(x => x.Name).ToList<object>());
            }
            //creating rows
            foreach (var entity in entities)
            {
                var values = GetObjectValues(entity);
                dt.Add(values);
            }


            return dt;
        }
        
        public static List<object> GetObjectValues<T>(T entity)
        {
            var values = new List<object>();
            foreach (var prop in typeof(T).GetProperties())
            {
                values.Add(prop.GetValue(entity));
            }

            return values.ToList();
        }
        
        public static List<Models.ExchangeRate> ParseExchangeRatesArs(this List<IList<object>> data)
        {
            List<Models.ExchangeRate> rates = new List<Models.ExchangeRate>();
            for(int i = 0; i < data[0].Count; i++)
            {
                if (data[1][i].ToString().Contains('-'))
                {
                    rates.Add(new Models.ExchangeRate("Argentina", "ARG", DateTime.Parse(data[0][i].ToString()).Date, Convert.ToDecimal("0"), "USD", "ARS"));
                }
                else
                {
                    rates.Add(new Models.ExchangeRate("Argentina", "ARG", DateTime.Parse(data[0][i].ToString()).Date, Convert.ToDecimal(data[1][i]), "USD", "ARS"));
                }
                
            }
            return rates;
        }

        public static List<Models.PartnersOperation> ParseExpenses(this List<IList<object>> data, Random rnd)
        {
            var operations = new List<Models.PartnersOperation>();
            

            for (int i = 1; i < data[0].Count; i++)
            {
                for (int j = 1; j < data.Count; j++)
                {
                    if (data[j][i].ToString().Any(char.IsDigit) && Convert.ToDecimal(data[j][i]) != (decimal) 0)
                    {

                        var date = DateTime.Parse(data[0][i].ToString()).AddHours(22).AddMinutes(rnd.Next(60)).AddSeconds(rnd.Next(60)).AddMilliseconds(rnd.Next(1000));

                        if (Convert.ToDecimal(data[j][i])<0)
                        {
                            operations.Add(new Models.PartnersOperation(date, "CREDIT", data[j][0].ToString().Trim(), "ARS", null, "REMITEE", Convert.ToDecimal(data[j][i]), null));
                        }
                        else
                        {
                            operations.Add(new Models.PartnersOperation(date, "DEBIT", data[j][0].ToString().Trim(), "ARS", null, "REMITEE", Convert.ToDecimal(data[j][i]), null));
                        }
                    }
                }
            }

            return operations;
        }

        public static List<Models.PartnersOperation> ParseExchanges(this List<IList<object>> data, Random rnd)
        {
            var operations = new List<Models.PartnersOperation>();


            for (int j = 1; j < data.Count; j++)
            {
                for (int i = 1; i < data[j].Count; i++)
                {
                    if (data[j][i].ToString().Any(char.IsDigit) && Convert.ToDecimal(data[j][i]) != (decimal)0)
                    {
                        var date = DateTime.Parse(data[0][i].ToString()).AddHours(22).AddMinutes(rnd.Next(60)).AddSeconds(rnd.Next(60)).AddMilliseconds(rnd.Next(1000));

                        if (Convert.ToDecimal(data[j][i]) < 0)
                        {
                            if (j < 2)
                            {
                                operations.Add(new Models.PartnersOperation(date, "DEBIT", "CCL Buy - PORTFOLIO INVESTMENTS", "USD", "ARS", "REMITEE", Convert.ToDecimal(data[j][i]), null));
                            }
                            else if (j < 4)
                            {
                                operations.Add(new Models.PartnersOperation(date, "DEBIT", "CCL Buy - BALANZ", "USD", "ARS", "REMITEE", Convert.ToDecimal(data[j][i]), null));
                            }
                            else if (j < 6)
                            {
                                operations.Add(new Models.PartnersOperation(date, "DEBIT", "CCL Buy - LINX", "USD", "ARS", "REMITEE", Convert.ToDecimal(data[j][i]), null));
                            }
                            else if (j < 8)
                            {
                                operations.Add(new Models.PartnersOperation(date, "DEBIT", "Intereses", "ARS", null, "REMITEE", Convert.ToDecimal(data[j][i]), null));
                            }
                            else
                            {
                                operations.Add(new Models.PartnersOperation(date, "DEBIT", "CCL Buy - BITZO", "USD", "ARS", "REMITEE", Convert.ToDecimal(data[j][i]), null));
                            }
                        }
                        else
                        {
                            if (j < 2)
                            {
                                operations.Add(new Models.PartnersOperation(date, "CREDIT", "CCL Buy - PORTFOLIO INVESTMENTS", "USD", "ARS", "REMITEE", Convert.ToDecimal(data[j][i]), Convert.ToDecimal(data[j + 1][i])));
                            }
                            else if (j < 4)
                            {
                                operations.Add(new Models.PartnersOperation(date, "CREDIT", "CCL Buy - BALANZ", "USD", "ARS", "REMITEE", Convert.ToDecimal(data[j][i]), Convert.ToDecimal(data[j + 1][i])));
                            }
                            else if (j < 6)
                            {
                                operations.Add(new Models.PartnersOperation(date, "CREDIT", "CCL Buy - LINX", "USD", "ARS", "REMITEE", Convert.ToDecimal(data[j][i]), Convert.ToDecimal(data[j + 1][i])));
                            }
                            else if (j < 8)
                            {
                                operations.Add(new Models.PartnersOperation(date, "CREDIT", "Intereses", "ARS", null, "REMITEE", Convert.ToDecimal(data[j][i]), null));
                            }
                            else
                            {
                                operations.Add(new Models.PartnersOperation(date, "CREDIT", "CCL Buy - BITZO", "USD", "ARS", "REMITEE", Convert.ToDecimal(data[j][i]), Convert.ToDecimal(data[j + 1][i])));
                            }
                            
                        } 
                    }
                }
                j++;
            }

            return operations;
        }

        public static List<Models.PartnersOperation> ParseObPartners(this List<IList<object>> data)
        {
            var operations = new List<Models.PartnersOperation>();


            for (int i = 2; i < data[0].Count; i++)
            {
                for (int j = 1; j < data.Count; j++)
                {
                    if (data[j][i].ToString().Any(char.IsDigit) 
                        && decimal.Parse(data[j][i].ToString() ?? "0") != (decimal)0 
                        && data[j][0].ToString() != "" 
                        && data[j][1].ToString() != "" 
                        && data[j][1].ToString() != "INGRESOS" 
                        && data[j][1].ToString() != "EGRESOS" 
                        && data[j][1].ToString() != "DISPONIBLE AL CIERRE" 
                        && data[j][1].ToString() != "Pago Principal")
                    {

                        operations.Add(new Models.PartnersOperation(DateTime.Parse(data[0][i].ToString()), "CREDIT", data[1][j].ToString(), "USD", "ARS", data[0][j].ToString(), Convert.ToDecimal(data[j][i]), null));
                        
                    }
                }
            }

            return operations;
        }

        public static DateTime FirstDayOfMonth(this DateTime date)
        {
            DateTime result = new DateTime(date.Year, date.Month, 1);
            return result;
        }

        public static object ParseCell(this ClosedXML.Excel.IXLCell cell)
        {
            if(cell.Value.IsDateTime)
            {
                return (object)cell.GetDateTime();
            }
            else if(cell.Value.IsNumber)
            {
                return (object) cell.GetDouble();
            }
            else if(cell.Value.IsText)
            {
                return (object)cell.GetString();
            }
            else
            {
                return (object) "";
            }
        }
    }
}
