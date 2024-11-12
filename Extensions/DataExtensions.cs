using Dapper;
using Microsoft.EntityFrameworkCore;
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
                    rates.Add(new Models.ExchangeRate("Argentina", "ARG", DateTime.Parse(data[0][i].ToString()).Date, Convert.ToDecimal("0"), "USD", "CCL"));
                }
                else
                {
                    rates.Add(new Models.ExchangeRate("Argentina", "ARG", DateTime.Parse(data[0][i].ToString()).Date, Convert.ToDecimal(String.IsNullOrEmpty(data[1][i].ToString()) ? "0" : data[1][i]) , "USD", "CCL"));
                }
                
            }
            return rates;
        }

        public static List<Models.ExchangeRate> ParseExchangeRatesVendor(this List<List<object>> data, string countryName, string countryCode, string sourceCurrency, string targetCurrency, string vendor = null)
        {
            List<Models.ExchangeRate> rates = new List<Models.ExchangeRate>();
            for (int i = 0; i < data[0].Count; i++)
            {
                DateTime date = new DateTime();
                if (DateTime.TryParse(data[0][i].ToString(), out date))
                {
                    if (data[1][i].ToString().Contains('-'))
                    {
                        rates.Add(new Models.ExchangeRate(countryName, countryCode, date.Date, Convert.ToDecimal("0"), sourceCurrency, targetCurrency, vendor));
                    }
                    else
                    {
                        rates.Add(new Models.ExchangeRate(countryName, countryCode, date.Date, Convert.ToDecimal(String.IsNullOrEmpty(data[1][i].ToString()) ? "0" : data[1][i]), sourceCurrency, targetCurrency, vendor));
                    }
                }
                

            }
            return rates;
        }

        public static List<Models.PartnersOperation> ParseExpenses(this List<IList<object>> data, Random rnd, string currency, DateTime dateFrom, DateTime dateTo)
        {
            var operations = new List<Models.PartnersOperation>();
            int datesRowIndex = 0;
            int datesStart = 0;
            int datesEnd = 0;

            var comitentesIndex = new Dictionary<string, ComitentesIndexes>();
            int start = 0;
            int finish = 1000;
            string title = "";
            List<string> expenses = new List<string> { "Comision Banco", 
                "Impuestos (IVA/IIBB)", 
                "Ley 25.413", 
                "Pago a Proveedores", 
                "Haberes", 
                "VEPs / Taxs", 
                "Otros Gastos bancarios" };

            DateTime dateFromParsed;
            DateTime dateToParsed;
            for (int j = 0; j < data.Count; j++)
            {
                if (data[j][10].GetType() == typeof(DateTime))
                {
                    datesRowIndex = j;
                    for (int k = 0; k < data[j].Count; k++)
                    {
                        var parsed = DateTime.TryParse(data[j][k].ToString(), out dateFromParsed);
                        if (parsed && dateFromParsed == dateFrom)
                        {
                            datesStart = k;
                        }
                        parsed = DateTime.TryParse(data[j][k].ToString(), out dateToParsed);
                        if (parsed && dateToParsed == dateTo.AddDays(-1))
                        {
                            datesEnd = k + 1;
                            break;
                        }
                    }
                    break;
                }
            }
            for (int j = 0; j < data.Count; j++)
            {
                if (data[j][3].ToString().Contains("Disponibilidades al inicio"))
                {
                    start = j;
                }
                if (data[j][3].ToString().Contains("Disponibilidades al cierre"))
                {
                    finish = j;
                    title = "";
                    for (int k = start; k < finish; k++)
                    {
                        title = title + data[k][2].ToString();
                        if (title == "TOTAL" + currency.ToUpper())
                        {
                            goto blockFound;
                        }
                    }
                }
            }
        blockFound:

            var relevantRows = new List<int>();

            for (int j = start; j < finish; j++)
            {
                if (expenses.Contains(data[j][3].ToString().TrimStart()))
                {
                    relevantRows.Add(j);
                }
            }
            decimal value = 0;
            for (int i = datesStart; i < datesEnd; i++)
            {
                foreach (var j in relevantRows)
                {                                 
                    if (Decimal.TryParse(data[j][i].ToString(),out value) && value != (decimal)0)
                    {

                        var date = DateTime.Parse(data[0][i].ToString()).AddHours(22).AddMinutes(rnd.Next(60)).AddSeconds(rnd.Next(60)).AddMilliseconds(rnd.Next(1000));

                        operations.Add(new Models.PartnersOperation(date, "DEBIT", data[j][3].ToString().Trim(), currency, null, "REMITEE", Convert.ToDecimal(data[j][i]), null, null));

                    }                     
                }
            }

            return operations;
        }

        public static List<Models.PartnersOperation> ParseExchanges(this List<IList<object>> data, Random rnd, List<ExchangeRate> exRateOf, DateTime dateFrom, DateTime dateTo)
        {
            var operations = new List<Models.PartnersOperation>();
            int datesRowIndex = 0;
            int datesStart = 0;
            int datesEnd = 0;

            var comitentesIndex = new Dictionary<string, ComitentesIndexes>();
            int start = 0;
            int finish = 1000;

            DateTime dateFromParsed;
            DateTime dateToParsed;
            for (int j = 0; j < data.Count; j++)
            {
                if (data[j][10].GetType() == typeof(DateTime))
                {
                    datesRowIndex = j;
                    for(int k = 0; k < data[j].Count; k++)
                    {
                        var parsed = DateTime.TryParse(data[j][k].ToString(), out dateFromParsed);
                        if (parsed && dateFromParsed == dateFrom)
                        {
                            datesStart = k;
                        }
                        parsed = DateTime.TryParse(data[j][k].ToString(), out dateToParsed);
                        if (parsed && dateToParsed == dateTo.AddDays(-1))
                        {
                            datesEnd = k + 1;
                            break;
                        }
                    }
                    break;
                }
            }
            for (int j = 0; j < data.Count; j++)
            {
                if (data[j][0].ToString().Contains("Remitee SA") || data[j][0].ToString().Contains("Remitee SPA"))
                {
                    if(!data[j-1][0].ToString().Contains("Remitee SA") && !data[j-1][0].ToString().Contains("Remitee SPA"))
                    {
                        start = j;
                    }
                    if (!data[j+1][0].ToString().Contains("Remitee SA") && !data[j+1][0].ToString().Contains("Remitee SPA"))
                    {
                        finish = j;
                        
                        var key = "";
                        var currency = "";
                        for (int k = start; k < finish; k++)
                        {
                            if (data[k][1].ToString().Length > 2)
                            {
                                key = key + data[k][1].ToString();
                            }
                            if (data[k][2].ToString() == "ar$")
                            {
                                currency = "ARS";
                            }
                            if (data[k][2].ToString() == "CLP")
                            {
                                currency = "CLP";
                            }

                        }
                        comitentesIndex[key] = new ComitentesIndexes { AmountIndex = 0, CurrencyIndex = currency, ExRateIndex = 0 };
                        for (int k = start; k < finish; k++)
                        {
                            if (data[k][3].ToString().Contains("Operado en ar$ Bonos"))
                            {
                                comitentesIndex[key].AmountIndex = k;
                            }
                            if (data[k][3].ToString().Contains("Operado en USD Bonos"))
                            {
                                comitentesIndex[key].ExRateIndex = k;
                                if (currency == "CLP")
                                {
                                    comitentesIndex[key].ExRateIndex = k + 3;
                                }
                            }
                            if (data[k][3].ToString().Contains("Operado en Clp Bonos"))
                            {
                                comitentesIndex[key].AmountIndex = k;
                            }
                            if (data[k][3].ToString().Contains("Intereses"))
                            {
                                comitentesIndex[key].AmountIndex = k;
                            }
                        }
                    }
                    
                }
                
            }

            foreach( var key in comitentesIndex.Keys )
            {
                for(int i = datesStart; i < datesEnd; i++)
                {
                    if (comitentesIndex[key].AmountIndex > 0 && data[comitentesIndex[key].AmountIndex][i].ToString().Any(char.IsDigit) && Convert.ToDecimal(data[comitentesIndex[key].AmountIndex][i]) != (decimal)0)
                    {
                        var date = DateTime.Parse(data[datesRowIndex][i].ToString()).AddHours(18).AddMinutes(rnd.Next(60)).AddSeconds(rnd.Next(60)).AddMilliseconds(rnd.Next(1000));
                        if (Convert.ToDecimal(data[comitentesIndex[key].AmountIndex][i]) < 0)
                        {
                                
                            if(comitentesIndex[key].ExRateIndex == 0)
                            {
                                operations.Add(new Models.PartnersOperation(date, "DEBIT", key, comitentesIndex[key].CurrencyIndex, null, "REMITEE", Math.Abs(Convert.ToDecimal(data[comitentesIndex[key].AmountIndex][i])), null, null));
                            }
                            else
                            {
                                operations.Add(new Models.PartnersOperation(date, "DEBIT", key, "USD", comitentesIndex[key].CurrencyIndex, "REMITEE", Math.Abs(Convert.ToDecimal(data[comitentesIndex[key].AmountIndex][i])), null, null));
                            }
                        }
                        else
                        {
                            var exRate = exRateOf.Where(x => x.Date == date.Date && x.TargetCurrency == "ARS").FirstOrDefault()?.ExchangeRate1;
                            var clpExRate = exRateOf.Where(x => x.Date == date.Date && x.TargetCurrency == "CLP").FirstOrDefault().ExchangeRate1;
                            if (comitentesIndex[key].ExRateIndex == 0)
                            {
                                operations.Add(new Models.PartnersOperation(date, "CREDIT", key, comitentesIndex[key].CurrencyIndex, null, "REMITEE", Convert.ToDecimal(data[comitentesIndex[key].AmountIndex][i]), null, null));
                            }
                            else
                            {
                                operations.Add(new Models.PartnersOperation(date, "CREDIT", key, "USD", comitentesIndex[key].CurrencyIndex, "REMITEE", Convert.ToDecimal(data[comitentesIndex[key].AmountIndex][i]), Convert.ToDecimal(data[comitentesIndex[key].ExRateIndex + 1][i]),exRate));
                            }
                        }
                        

                    }
                }
            }


            

            return operations;
        }

        public static List<Models.PartnersOperation> ParseBitsoExchanges(this List<IList<object>> data, List<IList<object>> usdData,Random rnd, List<ExchangeRate> exRateOf, DateTime dateFrom, DateTime dateTo)
        {
            var operations = new List<Models.PartnersOperation>();
            int datesRowIndex = 0;
            int datesStart = 0;
            int datesEnd = 0;

            int datesRowIndexUsd = 0;
            int datesStartUsd = 0;
            int datesEndUsd = 0;

            var comitentesIndex = new Dictionary<string, ComitentesIndexes>();
            int start = 0;
            int finish = 1000;
            int dataRow = 0;
            int dataRowUsd = 0;

            DateTime dateFromParsed;
            DateTime dateToParsed;
            for (int j = 0; j < data.Count; j++)
            {
                if (data[j][10].GetType() == typeof(DateTime))
                {
                    datesRowIndex = j;
                    for (int k = 0; k < data[j].Count; k++)
                    {
                        var parsed = DateTime.TryParse(data[j][k].ToString(), out dateFromParsed);
                        if (parsed && dateFromParsed == dateFrom)
                        {
                            datesStart = k;
                        }
                        parsed = DateTime.TryParse(data[j][k].ToString(), out dateToParsed);
                        if (parsed && dateToParsed == dateTo.AddDays(-1))
                        {
                            datesEnd = k + 1;
                            break;
                        }
                    }
                    break;
                }
            }
            for (int j = 0; j < usdData.Count; j++)
            {
                if (usdData[j][10].GetType() == typeof(DateTime))
                {
                    datesRowIndexUsd = j;
                    for (int k = 0; k < usdData[j].Count; k++)
                    {
                        var parsed = DateTime.TryParse(usdData[j][k].ToString(), out dateFromParsed);
                        if (parsed && dateFromParsed == dateFrom)
                        {
                            datesStartUsd = k;
                        }
                        parsed = DateTime.TryParse(usdData[j][k].ToString(), out dateToParsed);
                        if (parsed && dateToParsed == dateTo.AddDays(-1))
                        {
                            datesEndUsd = k + 1;
                            break;
                        }
                    }
                    break;
                }
            }

            var titleRow = data.Select(x => x[2].ToString()).ToList().IndexOf("BITSO ARS");
            dataRow = titleRow;
            for(int j = titleRow; j > 0; j--)
            {
                if (data[j][3].ToString().TrimStart() == "Ingresos Financieros")
                {
                    dataRow = j;
                    break;
                }
            }
            var titleRowUsd = usdData.Select(x => x[2].ToString()).ToList().IndexOf("BITSO USD");
            dataRowUsd = titleRowUsd;
            for (int j = titleRowUsd; j < 1000; j++)
            {
                if (usdData[j][3].ToString().TrimStart() == "Operaciones Comex")
                {
                    dataRowUsd = j;
                    break;
                }
            }

            for (int i = datesStart; i < datesEnd; i++)
            {
                
                if (data[dataRow][i].ToString().Any(char.IsDigit) && Convert.ToDecimal(data[dataRow][i]) != (decimal)0
                    && usdData[dataRowUsd][i].ToString().Any(char.IsDigit) && Convert.ToDecimal(usdData[dataRowUsd][i]) != (decimal)0)
                {
                    var date = DateTime.Parse(data[datesRowIndex][i].ToString()).AddHours(18).AddMinutes(rnd.Next(60)).AddSeconds(rnd.Next(60)).AddMilliseconds(rnd.Next(1000));
                    var exRate = exRateOf.Where(x => x.Date == date.Date && x.TargetCurrency == "ARS").FirstOrDefault().ExchangeRate1;
                    operations.Add(new Models.PartnersOperation(date, "CREDIT", "BITSO", "USD", "ARS", "REMITEE", Convert.ToDecimal(data[dataRow][i]), Convert.ToDecimal(data[dataRow][i]) / Convert.ToDecimal(usdData[dataRowUsd][i]), exRate));
                }
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

                        operations.Add(new Models.PartnersOperation(DateTime.Parse(data[0][i].ToString()), "CREDIT", data[1][j].ToString(), "USD", "ARS", data[0][j].ToString(), Convert.ToDecimal(data[j][i]), null, null));
                        
                    }
                }
            }

            return operations;
        }

        public static List<Models.PartnersOperation> ParseIbPartnerWires(this List<IList<object>> data, Random rnd)
        {
            var operations = new List<Models.PartnersOperation>();
            DateTime date;
            decimal amount;
            decimal? exRate;

            for (int i = 0; i < data.Count; i++)
            {
                if(DateTime.TryParse(data[i][0].ToString(), out date))
                {
                    amount = data[i][3].GetType() == typeof(decimal) ? Convert.ToDecimal(data[i][3]) : (data[i][4].GetType() == typeof(decimal) ? Convert.ToDecimal(data[i][4]) : (data[i][5].GetType() == typeof(decimal) ? Convert.ToDecimal(data[i][5]) : (decimal)0));
                    exRate = data[i][12].GetType() == typeof(decimal) ? Convert.ToDecimal(data[i][12]) : null;
                    operations.Add(new PartnersOperation(date, "CREDIT", "IB Wire", data[i][6].ToString(), data[i][6].ToString(), data[i][2].ToString(), amount, exRate, null));
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
            try
            {
                if (cell.Value.IsDateTime)
                {
                    return (object)cell.GetDateTime();
                }
                else if (cell.Value.IsNumber)
                {
                    return (object)cell.GetDouble();
                }
                else if (cell.Value.IsText)
                {
                    return (object)cell.GetString();
                }
                else
                {
                    return (object)"";
                }
            }
            catch(Exception ex)
            {
                return (object)"";
            }
            
        }

        public static List<FlatTransaction> AddCollectorDataExtension(this List<FlatTransaction> list, DateTime dateFrom, DateTime dateTo, RemiteeServicesMetricsContext ctx)
        {
            
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
                else if (tcReferences.Select(x => x.ReferenceId?.ToUpper()).Contains(item.MoneyTransferPaymentId.ToString()?.ToUpper()))
                {
                    item.TransactionCollectorTransactionId = tcReferences.First(x => x.ReferenceId?.ToUpper() == item.MoneyTransferPaymentId.ToString()?.ToUpper()).Id;
                    item.SenderUniqueId = Guid.Parse(tcReferences.First(x => x.ReferenceId?.ToUpper() == item.MoneyTransferPaymentId.ToString()?.ToUpper()).SenderId);
                    item.ReceiverUniqueId = Guid.Parse(tcReferences.First(x => x.ReferenceId?.ToUpper() == item.MoneyTransferPaymentId.ToString()?.ToUpper()).ReceiverId);
                    item.ProcessedInCollector = true;
                }
                
            }

            return list;
        }

        public static IEnumerable<IEnumerable<T>> ToChunks<T>(this IEnumerable<T> enumerable,
                                                      int chunkSize)
        {
            int itemsReturned = 0;
            var list = enumerable.ToList(); // Prevent multiple execution of IEnumerable.
            int count = list.Count;
            while (itemsReturned < count)
            {
                int currentChunkSize = Math.Min(chunkSize, count - itemsReturned);
                yield return list.GetRange(itemsReturned, currentChunkSize);
                itemsReturned += currentChunkSize;
            }
        }

        public static List<List<T>> ChunkBy<T>(this List<T> source, int chunkSize)
        {
            return source
                .Select((x, i) => new { Index = i, Value = x })
                .GroupBy(x => x.Index / chunkSize)
                .Select(x => x.Select(v => v.Value).ToList())
                .ToList();
        }

        public static List<TransactionalBase> ProcessFx(this List<TransactionalBase> list)
        {
            foreach(var tb in list)
            {
                if (tb.Source == "MoneyTransfer")
                {
                    if (tb.Client == "REMITEE")
                    {
                        if (tb.SourceCurrency == "ARS" && tb.TargetCurrency == "ARS")
                        {
                            tb.NetAmountUsd = tb.NetAmountSc / tb.ArsexchangeRate;
                        }
                        if (tb.SourceCurrency == "CLP" && tb.TargetCurrency == "ARS")
                        {
                            tb.NetAmountUsd = tb.NetAmountSc / tb.ExchangeRateSc;
                        }  
                    }

                }
                if (tb.Source == "Ledger")
                {
                    if (tb.TargetCountryCode == "ARG")
                    {
                        tb.NetAmountUsd = tb.TargetAmountTc / tb.ArsexchangeRate;
                    }
                    if (tb.TargetCountryCode == "CHL")
                    {
                        tb.NetAmountUsd = tb.TargetAmountTc / tb.ClpexchangeRate;
                    }
                }
                if (tb.Source == "Wallet")
                {
                    if (tb.SourceCurrency == "CLP")
                    {
                        tb.NetAmountUsd = tb.NetAmountSc / tb.ClpexchangeRate;
                    }
                    if (tb.SourceCurrency == "ARS")
                    {
                        tb.NetAmountUsd = tb.NetAmountSc / tb.ArsexchangeRate;
                    }
                    if (tb.SourceCurrency != "CLP" && tb.SourceCurrency != "ARS")
                    {
                        tb.NetAmountUsd = 0;
                    }
                }
                if (tb.Source == "MoneyTransfer")
                {
                    if (tb.Client == "REMITEE" && tb.SourceCurrency == "ARS")
                    {
                        tb.FeeAmountUsd = tb.FeeAmountSc / tb.ArsexchangeRate;
                        tb.Vatusd = tb.Vatrate * tb.FeeAmountSc / tb.ArsexchangeRate;
                    }
                    if (tb.Client == "REMITEE" && tb.SourceCurrency == "CLP")
                    {
                        tb.FeeAmountUsd = tb.FeeAmountSc / tb.ExchangeRateSc;
                        tb.Vatusd = tb.Vatrate * tb.FeeAmountSc / tb.ExchangeRateSc;
                    }
                }
                if (tb.Source == "Ledger")
                {
                    if (tb.TargetCountryCode == "ARG")
                    {
                        tb.FeeAmountUsd = tb.TargetAmountTc * tb.FeeRate / tb.ArsexchangeRate;
                        tb.Vatusd = tb.Vatrate * tb.TargetAmountTc * tb.FeeRate / tb.ArsexchangeRate;
                    }
                    if (tb.TargetCountryCode == "CHL")
                    {
                        tb.FeeAmountUsd = tb.TargetAmountTc * tb.FeeRate / tb.ClpexchangeRate;
                        tb.Vatusd = tb.Vatrate * tb.TargetAmountTc * tb.FeeRate / tb.ClpexchangeRate;
                    }
                    if (tb.TargetCountryCode != "ARG" && tb.TargetCountryCode != "CHL")
                    {
                        tb.FeeAmountUsd = tb.NetAmountUsd * tb.FeeRate;
                        tb.Vatusd = tb.Vatrate * tb.NetAmountUsd * tb.FeeRate;
                    }
                }
                if (tb.Source == "Wallet")
                {
                    if (tb.SourceCurrency == "CLP")
                    {
                        tb.FeeAmountUsd = tb.NetAmountSc * (decimal)0.11 / tb.ClpexchangeRate;
                        tb.Vatusd = 0;
                    }
                    if (tb.SourceCurrency == "ARS")
                    {
                        tb.FeeAmountUsd = tb.NetAmountSc * (decimal)0.13 / tb.ArsexchangeRate;
                        tb.Vatusd = tb.NetAmountSc * (decimal)0.03 * tb.Vatrate / tb.ArsexchangeRate;
                    }
                    if (tb.SourceCurrency != "CLP" && tb.SourceCurrency != "ARS")
                    {
                        tb.FeeAmountUsd = 0;
                    }
                }
                if (tb.Source == "MoneyTransfer")
                {
                    if (tb.Client == "REMITEE")
                    {
                        if (tb.SourceCurrency == "ARS")
                        {
                            if (tb.TargetCurrency == "ARS")
                            {
                                tb.SpreadAmountUsd = tb.NetAmountSc / tb.ArsexchangeRate - tb.TargetAmountTc / tb.ArsexchangeRate;
                            }
                            else
                            {
                                tb.SpreadAmountUsd = tb.NetAmountSc / tb.ArsexchangeRate - tb.NetAmountUsd;
                            }
                        }
                        else
                        {
                            if (tb.TargetCurrency == "ARS")
                            {
                                tb.SpreadAmountUsd = tb.NetAmountSc / tb.ExchangeRateSc - tb.TargetAmountTc / tb.ArsexchangeRate;
                            }
                            else
                            {
                                tb.SpreadAmountUsd = tb.NetAmountSc / tb.ExchangeRateSc - tb.NetAmountUsd;
                            }
                        }
                    }
                    else
                    {
                        if (tb.TargetCurrency == "ARS")
                        {
                            tb.SpreadAmountUsd = tb.NetAmountUsd - tb.TargetAmountTc / tb.ArsexchangeRate;
                            tb.SpreadAmountSc = tb.NetAmountUsd - tb.TargetAmountTc / tb.ArsexchangeRate;
                        }
                        else if (tb.SpreadRate == null)
                        {
                            tb.SpreadAmountUsd = 0;
                        }
                        else if (tb.TargetCurrency == "VEF")
                        {

                            tb.SpreadAmountUsd = tb.NetAmountUsd * tb.SpreadRate;

                        }
                        else
                        {
                            tb.SpreadAmountUsd = tb.NetAmountUsd - tb.TargetAmountTc / tb.MarketExchangeRate;
                        }
                    }
                }
            }

            return list;
        }

        public static List<TransactionalBase> AddExchangeRateOf(this List<TransactionalBase> list, DateTime dateFrom, DateTime dateTo, IConfigurationRoot config)
        {
            var arsOfExRates = new List<ExchangeRate>();

            using(var ctx = new RemiteeServicesMetricsContext(config))
            {
                arsOfExRates = ctx.ExchangeRates.Where(x => x.Date >= dateFrom
                && x.Date <= dateTo
                && x.TargetCurrency == "ARS").ToList();
            }
            foreach(var item in list)
            {
                item.ArsExchangeRateOf = arsOfExRates.Where(x => x.Date == item.CreatedAt.Date).FirstOrDefault().ExchangeRate1;
            }

            return list;
        }

        

        public static List<TransactionalBase> AddExchangeRates(this List<TransactionalBase> list, 
            DateTime dateFrom, 
            DateTime dateTo, 
            IConfigurationRoot config, 
            bool includeArsExchangeRate = true,
            bool includeClpExchangeRate = true,
            bool includeArsOfExchangeRate = true,
            bool includeMarketExchangeRate = true)
        {
            var arsExRates = new List<ExchangeRate>();
            var arsOfExRates = new List<ExchangeRate>();
            var clpExRates = new List<ExchangeRate>();
            var marketExRate = new List<ExchangeRate>();

            using (var ctx = new RemiteeServicesMetricsContext(config))
            {
                ctx.ChangeTracker.AutoDetectChangesEnabled = false;
                ctx.Database.SetCommandTimeout(300);

                arsExRates = ctx.ExchangeRates.Where(x => x.Date >= dateFrom
                && x.Date <= dateTo
                && x.TargetCurrency == "CCL").ToList();
                arsOfExRates = ctx.ExchangeRates.Where(x => x.Date >= dateFrom
                && x.Date <= dateTo
                && x.TargetCurrency == "ARS").ToList();
                clpExRates = ctx.ExchangeRates.Where(x => x.Date >= dateFrom
                && x.Date <= dateTo
                && x.TargetCurrency == "CLP").ToList();
                marketExRate = ctx.ExchangeRates.Where(x => x.Date >= dateFrom
                && x.Date <= dateTo
                && x.TargetCurrency != "CCL").ToList();
            }
            foreach (var item in list)
            {
                if(includeArsExchangeRate)
                {
                    item.ArsexchangeRate = arsExRates.Where(x => x.Date == item.CreatedAt.Date).FirstOrDefault().ExchangeRate1;
                }
                if(includeClpExchangeRate)
                {
                    item.ClpexchangeRate = clpExRates.Where(x => x.Date == item.CreatedAt.Date).FirstOrDefault().ExchangeRate1;
                }
                if(includeArsOfExchangeRate)
                {
                    item.ArsExchangeRateOf = arsOfExRates.Where(x => x.Date == item.CreatedAt.Date).FirstOrDefault().ExchangeRate1;
                }
                if(includeMarketExchangeRate)
                {
                    if (item.MarketExchangeRate is null && item.TargetCurrency != "USD")
                    {
                        item.MarketExchangeRate = marketExRate.Where(x => x.Date == item.CreatedAt.Date && x.TargetCurrency == item.TargetCurrency).FirstOrDefault().ExchangeRate1;
                    }
                }
            }
            return list;
        }

        

        

        public static List<List<T>> Pivot<T>(this List<IList<T>> source)
        {
            var result = source.SelectMany(inner => inner.Select((item, index) => new { item, index }))
                .GroupBy(i => i.index, i => i.item)
                .Select(g => g.ToList())
                .ToList();
            return result;
        }
    }
}
