
using System;
using System.Data.SqlClient;
using Remitee.Services.Metrics.Controllers;
using Remitee.Services.Metrics.Models;
using System.Data;
using Remitee.Services.Metrics.Extensions;
using Microsoft.Extensions.Configuration;
using Remitee.Services.Metrics.Services;

namespace DataExport
{
    class Program
    {
        static async Task Main(string[] args)
        {

            var dateFrom = new DateTime(2024, 11, 1);
            var dateTo = new DateTime(2024, 11, 11);
            


            var builder = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);
            var config = builder.Build();
            var importService = new ImportService(config);
            var processingService = new ProcessingService(config);
            var exportService = new ExportService(config);

            var tcService = new FlatTransactionsController(config);

            var connector = new SpreadSheetConnector(config);

            connector.fileLocation = await connector.DownloadSharedFile("POSICION DIARIA REMITEE 2024.xlsx");
            //connector.DownloadSharedFile("POSICION DIARIA REMITEE 2024.xlsx").Wait();
            connector.fileLocation = @"C:\Users\FrancoZeppilli\Documents\Remitee\Innovacion\Metrics\Remitee.Services.Metrics\Descargas\POSICION DIARIA REMITEE 2024.xlsx";

            
            //Daily Commands
            Console.WriteLine("Import Started At: " + DateTime.Now.ToString());
            importService.UpdateTransactionalBase(dateFrom, dateTo);
            importService.UpdateTCCountries();
            importService.UpdateTCTables(dateFrom);
            importService.UpdateExchangeRatesFromDailyPosition(dateFrom, dateTo, connector);
            importService.UpdateExchangeRatesFromCurrencyLayer(dateFrom, dateTo, connector);
            importService.UpdatePartnersOperations(dateFrom, dateTo, connector);
            
            Console.WriteLine("Processing Started At: " + DateTime.Now.ToString());
            processingService.AddCountryNames();
            processingService.ModifyDates(dateFrom, dateTo);
            Console.WriteLine("Processing Ended At: " + DateTime.Now.ToString());

            //Tc service Commands
            tcService.UploadData(dateFrom, dateTo); //TODO: Aplicar BulkOperations
            tcService.AddCollectorData(dateFrom, dateTo);
            tcService.AddUsersReferences(dateFrom, dateTo);

            //Monthly Commands
            Console.WriteLine("Import Started At: " + DateTime.Now.ToString());
            importService.UpdateExchangeRatesFromDailyPosition(dateFrom, dateTo, connector);
            importService.UpdateExchangeRatesFromCurrencyLayer(dateFrom, dateTo, connector);
            importService.UpdatePartnersOperations(dateFrom, dateTo, connector);
            
            Console.WriteLine("Processing Started At: " + DateTime.Now.ToString());
            processingService.ModifyDates(dateFrom, dateTo);
            processingService.AddBillingInfo(dateFrom, dateTo);
            processingService.AddPayersReferences(dateFrom, dateTo);
            processingService.ModifyMarketExchangeRate(dateFrom, dateTo, connector);
            processingService.AddPayersDataFromConnectors(dateFrom, dateTo);
            
            processingService.ModifyPayerExchangeRate(dateFrom, dateTo, connector);
            processingService.AddAccountingPeriod(dateFrom.Year, dateFrom.Month);
            processingService.AddFixedDates(dateFrom, dateTo);
            
            
            processingService.AddArsRExchangeRate(dateFrom, dateTo);
            processingService.AddClpRExchangeRate(dateFrom, dateTo);
            processingService.AddSpreadDto(dateFrom, dateTo, ProcessingService.ObPartner.Italcambio);
            processingService.ModifyMissingPayerExchangeRates(dateFrom, dateTo);
            
            processingService.AddModifiedFields(dateFrom, dateTo);
            processingService.CalculateModifiedFields(dateFrom, dateTo);
            
            processingService.AddCalculatedPayerFees(dateFrom, dateTo);


            //Set in stone Commands
            exportService.GetCorredores(2024, 10);
            exportService.GetInbound(2024, 10);
            exportService.GetComplexChurn(2024, 10);


        }
    }
}

