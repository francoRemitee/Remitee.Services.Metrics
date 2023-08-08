
using System;
using System.Data.SqlClient;
using Remitee.Services.Metrics.Controllers;
using Remitee.Services.Metrics;
using Remitee.Services.Metrics.Models;
using System.Data;
using Remitee.Services.Metrics.Extensions;
using Microsoft.Extensions.Configuration;

namespace DataExport
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var dateTo = new DateTime(2023, 8, 1);//DateTime.UtcNow.Date;
            var dateFrom = new DateTime(2023, 7, 1);

            var service = new MetricsService();
            
            var builder = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);
            var config = builder.Build();
            var tcService = new FlatTransactionsController(config);

            
            
            var connector = new SpreadSheetConnector(config);
            connector.DownloadSharedFile().Wait();
            connector.fileLocation = @"C:\Users\FrancoZeppilli\Documents\Remitee\Innovacion\Metrics\Remitee.Services.Metrics\Descargas\POSICION DIARIA REMITEE 2023.xlsx";

            service.ReadExchangeRates(connector, dateFrom, dateTo);
            service.ReadPartnersOperations(connector, dateFrom, dateTo);
            service.UpdateTCTables(dateFrom);


            service.UpdateTransactionalBase(dateFrom, dateTo, config);
            service.AddArsRExchangeRate(dateFrom, dateTo, config);
            tcService.UploadData(dateFrom, dateTo);

            if (dateTo.Day <= 5)
            {
                service.UpdateBasicChurn(new List<int> { 1, 19, 4, 5, 18 }, dateFrom.Year, dateFrom.Month, config);
                service.UpdateSenders(dateFrom.Year, dateFrom.Month, config);
                service.UpdateNewSenders(dateFrom.Year, dateFrom.Month, config);
                service.UpdateReceivers(dateFrom.Year, dateFrom.Month, config);
                service.UpdateRegistrating(dateFrom.Year, dateFrom.Month, config);
                service.UpdateSendersBreakdown(new List<int> { 1, 19 }, dateFrom.Year, dateFrom.Month, config);
                service.UpdateCorredores(dateFrom.Year, dateFrom.Month);
                service.UpdateInbound(dateFrom.Year, dateFrom.Month);
                service.UpdateComplexChurn(dateFrom.Year, dateFrom.Month);
            }
            



            



        }
    }
}

