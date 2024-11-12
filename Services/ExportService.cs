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
    public class ExportService
    {
        private readonly IConfigurationRoot _configuration;
        public ExportService(IConfigurationRoot configuration)
        {
            _configuration = configuration;
        }
        public void GetCorredores(int year, int month)
        {
            var results = new List<Corredore>();

            using (var ctx = new RemiteeServicesMetricsContext(_configuration))
            {
                ctx.Corredores.Where(x => x.Year == year && x.Month == month).ExecuteDelete();
                var countries = ctx.Tccountries.ToList();
                var corredores = (from t in ctx.TransactionalBases
                                  join m in ctx.TbModifiedFields on t.Id equals m.TransactionalBaseId
                                  where t.AccountingPeriod == year.ToString("0000") + "-" + month.ToString("00")
                                  select new
                                  {
                                      t.CreatedAt,
                                      t.SourceCountryCode,
                                      t.TargetCountryCode,
                                      t.Client,
                                      t.CollectMethod,
                                      m.NetAmountUsd,
                                      m.IbFeeAmountUsd,
                                      m.IbSpreadAmountUsd,
                                      m.ObFeeAmountUsd,
                                      m.ObSpreadAmountUsd,
                                      m.IbVatUsd
                                  }).ToList()
                               .GroupBy(x => new
                               {
                                   x.CreatedAt.Year,
                                   x.CreatedAt.Month,
                                   x.SourceCountryCode,
                                   x.TargetCountryCode
                               });
                foreach (var key in corredores)
                {
                    results.Add(new Corredore(key.Key.Year,
                                            key.Key.Month,
                                            countries.Find(x => x.Id == key.Key.SourceCountryCode)?.Description ?? "",
                                            key.Key.SourceCountryCode ?? "",
                                            countries.Find(x => x.Id == key.Key.TargetCountryCode)?.Description ?? "",
                                            key.Key.TargetCountryCode ?? "",
                                            key.Count(),
                                            key.Where(x => x.Client == "REMITEE").Count(),
                                            key.Where(x => x.Client != "REMITEE").Count(),
                                            key.Where(x => x.CollectMethod == "TOPUP").Count(),
                                            key.Where(x => x.CollectMethod != "TOPUP").Count(),
                                            key.Where(x => x.Client == "REMITEE").Sum(x => x.NetAmountUsd) ?? 0,
                                            key.Where(x => x.Client != "REMITEE").Sum(x => x.NetAmountUsd) ?? 0,
                                            key.Sum(x => x.NetAmountUsd) ?? 0,
                                            key.Where(x => x.CollectMethod == "TOPUP").Sum(x => x.NetAmountUsd) ?? 0,
                                            key.Where(x => x.CollectMethod != "TOPUP").Sum(x => x.NetAmountUsd) ?? 0,
                                            key.Where(x => x.Client == "REMITEE").Sum(x => x.NetAmountUsd + x.IbFeeAmountUsd + x.IbVatUsd) ?? 0,
                                            key.Where(x => x.Client != "REMITEE").Sum(x => x.NetAmountUsd + x.IbFeeAmountUsd + x.IbVatUsd) ?? 0,
                                            key.Sum(x => x.NetAmountUsd + x.IbFeeAmountUsd + x.IbVatUsd) ?? 0,
                                            key.Where(x => x.CollectMethod == "TOPUP").Sum(x => x.NetAmountUsd + x.IbFeeAmountUsd + x.IbVatUsd) ?? 0,
                                            key.Where(x => x.CollectMethod != "TOPUP").Sum(x => x.NetAmountUsd + x.IbFeeAmountUsd + x.IbVatUsd) ?? 0,
                                            key.Where(x => x.Client == "REMITEE").Sum(x => x.IbFeeAmountUsd) ?? 0,
                                            key.Where(x => x.Client != "REMITEE").Sum(x => x.IbFeeAmountUsd) ?? 0,
                                            key.Sum(x => x.IbFeeAmountUsd),
                                            key.Where(x => x.Client == "REMITEE").Sum(x => x.IbSpreadAmountUsd) ?? 0,
                                            key.Where(x => x.Client != "REMITEE").Sum(x => x.IbSpreadAmountUsd) ?? 0,
                                            key.Sum(x => x.IbSpreadAmountUsd)
                                            ));
                }
                ctx.Corredores.AddRange(results);
                ctx.SaveChanges();
            }
        }

        public void GetInbound(int year, int month)
        {
            var results = new List<Inbound>();


            using (var ctx = new RemiteeServicesMetricsContext(_configuration))
            {
                ctx.Inbounds.Where(x => x.Year == year && x.Month == month).ExecuteDelete();
                var countries = ctx.Tccountries.ToList();
                var inbound = (from t in ctx.TransactionalBases
                               join m in ctx.TbModifiedFields on t.Id equals m.TransactionalBaseId
                               where t.AccountingPeriod == year.ToString("0000") + "-" + month.ToString("00")
                               select new
                               {
                                   t.CreatedAt,
                                   t.SourceCountryCode,
                                   t.TargetCountryCode,
                                   t.Client,
                                   t.CollectMethod,
                                   m.NetAmountUsd,
                                   m.IbFeeAmountUsd,
                                   m.IbSpreadAmountUsd,
                                   m.ObFeeAmountUsd,
                                   m.ObSpreadAmountUsd,
                                   m.IbVatUsd
                               }).ToList()
                               .GroupBy(x => new
                               {
                                   x.CreatedAt.Year,
                                   x.CreatedAt.Month,
                                   Partner = x.Client,
                                   x.SourceCountryCode,
                                   x.TargetCountryCode
                               });
                foreach (var key in inbound)
                {
                    results.Add(new Inbound(key.Key.Year,
                                            key.Key.Month,
                                            key.Key.Partner ?? "REMITEE",
                                            countries.Find(x => x.Id == key.Key.SourceCountryCode)?.Description ?? "",
                                            key.Key.SourceCountryCode ?? "",
                                            countries.Find(x => x.Id == key.Key.TargetCountryCode)?.Description ?? "",
                                            key.Key.TargetCountryCode ?? "",
                                            key.Count(),
                                            key.Where(x => x.CollectMethod == "TOPUP").Count(),
                                            key.Where(x => x.CollectMethod != "TOPUP").Count(),
                                            key.Sum(x => x.NetAmountUsd) ?? 0,
                                            key.Where(x => x.CollectMethod == "TOPUP").Sum(x => x.NetAmountUsd) ?? 0,
                                            key.Where(x => x.CollectMethod != "TOPUP").Sum(x => x.NetAmountUsd) ?? 0,
                                            key.Sum(x => x.NetAmountUsd + x.IbFeeAmountUsd + x.IbVatUsd) ?? 0,
                                            key.Where(x => x.CollectMethod == "TOPUP").Sum(x => x.NetAmountUsd + x.IbFeeAmountUsd + x.IbVatUsd) ?? 0,
                                            key.Where(x => x.CollectMethod != "TOPUP").Sum(x => x.NetAmountUsd + x.IbFeeAmountUsd + x.IbVatUsd) ?? 0,
                                            key.Average(x => x.NetAmountUsd),
                                            key.Sum(x => x.IbFeeAmountUsd),
                                            key.Sum(x => x.IbSpreadAmountUsd),
                                            key.Sum(x => x.IbVatUsd)));
                }
                ctx.Inbounds.AddRange(results);
                ctx.SaveChanges();
            }
        }

        public void GetComplexChurn(int year, int month)
        {

            var records = new List<ComplexChurn>();

            using (var ctx = new RemiteeServicesMetricsContext(_configuration))
            {

                ctx.Database.SetCommandTimeout(new TimeSpan(0, 20, 0));
                ctx.ComplexChurns.Where(x => x.CurrentMonth == month && x.CurrentYear == year).ExecuteDelete();

                var temp = (from s in ctx.FlatTransactions
                            join ft in ctx.Tctransactions.GroupBy(x => x.SenderId)
                                                         .Select(x => new { DateCreated = x.Min(y => y.DateCreated), SenderId = x.Key })
                                                         on s.SenderUniqueId.ToString() equals ft.SenderId
                            join tb in ctx.TransactionalBases on s.Id equals tb.Id
                            join m in ctx.TbModifiedFields on tb.Id equals m.TransactionalBaseId
                            where tb.AccountingPeriod == year.ToString("0000") + "-" + month.ToString("00")
                            select new
                            {
                                SenderId = s.SenderUniqueId,
                                tb.SourceCountryName,
                                tb.SourceCountryCode,
                                tb.TargetCountryName,
                                tb.TargetCountryCode,
                                tb.CreatedAt.Year,
                                tb.CreatedAt.Month,
                                firstTrxMonth = ft.DateCreated.Month,
                                firstTrxYear = ft.DateCreated.Year,
                                tb.Client,
                                tb.Status,
                                tb.CollectMethod,
                                GTV = m.NetAmountUsd
                            }).ToList();



                var churn = temp.GroupBy(x => new
                {
                    x.SenderId,
                    x.Client,
                    FirstTrxMonth = x.firstTrxMonth,
                    FirstTrxYear = x.firstTrxYear,
                    x.SourceCountryName,
                    x.SourceCountryCode,
                    x.TargetCountryName,
                    x.TargetCountryCode
                }).ToList();
                foreach (var key in churn)
                {
                    records.Add(new ComplexChurn("Sender",
                                key.Key.SenderId,
                                key.Key.SourceCountryName,
                                key.Key.SourceCountryCode,
                                key.Key.TargetCountryName,
                                key.Key.TargetCountryCode,
                                month,
                                year,
                                key.Key.FirstTrxMonth,
                                key.Key.FirstTrxYear,
                                key.Key.Client,
                                key.Where(x => x.Year == year && x.Month == month)
                                .Sum(x => x.GTV),
                                key.Where(x => x.Year == year && x.Month == month && x.CollectMethod == "TOPUP")
                                .Sum(x => x.GTV),
                                key.Where(x => x.Year == year && x.Month == month && x.CollectMethod == "MONEY TRANSFER")
                                .Sum(x => x.GTV),
                                key.Where(x => x.Year == year && x.Month == month)
                                .Count(),
                                key.Where(x => x.Year == year && x.Month == month && x.CollectMethod == "TOPUP")
                                .Count(),
                                key.Where(x => x.Year == year && x.Month == month && x.CollectMethod == "MONEY TRANSFER")
                                .Count(),
                                key.Where(x => x.Year == year && x.Month == month)
                                .Average(x => x.GTV)));

                }

                ctx.ComplexChurns.AddRange(records);
                ctx.SaveChanges();
                records.Clear();
            }

            using (var ctx = new RemiteeServicesMetricsContext(_configuration))
            {
                ctx.Database.SetCommandTimeout(new TimeSpan(0, 20, 0));
                var tempQ = from r in ctx.FlatTransactions
                            join ft in ctx.Tctransactions.GroupBy(x => x.ReceiverId)
                                                         .Select(x => new { DateCreated = x.Min(y => y.DateCreated), ReceiverId = x.Key })
                                                         on r.ReceiverUniqueId.ToString() equals ft.ReceiverId
                            join tb in ctx.TransactionalBases on r.Id equals tb.Id
                            join m in ctx.TbModifiedFields on tb.Id equals m.TransactionalBaseId
                            where tb.AccountingPeriod == year.ToString("0000") + "-" + month.ToString("00")
                            select new
                            {
                                ReceiverId = r.ReceiverUniqueId,
                                tb.SourceCountryName,
                                tb.SourceCountryCode,
                                tb.TargetCountryName,
                                tb.TargetCountryCode,
                                tb.CreatedAt.Year,
                                tb.CreatedAt.Month,
                                firstTrxMonth = ft.DateCreated.Month,
                                firstTrxYear = ft.DateCreated.Year,
                                tb.Client,
                                tb.Status,
                                tb.CollectMethod,
                                GTV = m.NetAmountUsd
                            };

                var query = tempQ.ToQueryString();
                var temp = tempQ.ToList();
                var churn = temp.GroupBy(x => new
                {
                    x.ReceiverId,
                    x.Client,
                    FirstTrxMonth = x.firstTrxMonth,
                    FirstTrxYear = x.firstTrxYear,
                    x.SourceCountryName,
                    x.SourceCountryCode,
                    x.TargetCountryName,
                    x.TargetCountryCode
                }).ToList();
                foreach (var key in churn)
                {

                    records.Add(new ComplexChurn("Receiver",
                                key.Key.ReceiverId,
                                key.Key.SourceCountryName,
                                key.Key.SourceCountryCode,
                                key.Key.TargetCountryName,
                                key.Key.TargetCountryCode,
                                month,
                                year,
                                key.Key.FirstTrxMonth,
                                key.Key.FirstTrxYear,
                                key.Key.Client,
                                key.Where(x => x.Year == year && x.Month == month).Sum(x => x.GTV),
                                key.Where(x => x.Year == year && x.Month == month && x.CollectMethod == "TOPUP").Sum(x => x.GTV),
                                key.Where(x => x.Year == year && x.Month == month && x.CollectMethod == "MONEY TRANSFER").Sum(x => x.GTV),
                                key.Where(x => x.Year == year && x.Month == month).Count(),
                                key.Where(x => x.Year == year && x.Month == month && x.CollectMethod == "TOPUP").Count(),
                                key.Where(x => x.Year == year && x.Month == month && x.CollectMethod == "MONEY TRANSFER").Count(),
                                key.Where(x => x.Year == year && x.Month == month).Average(x => x.GTV)));
                }

                ctx.ComplexChurns.AddRange(records);
                ctx.SaveChanges();
            }
        }

    }
}
