using Microsoft.EntityFrameworkCore;
using Remitee.Services.Metrics.Controllers;
using Remitee.Services.Metrics.Extensions;
using Remitee.Services.Metrics.Models;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
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

namespace Remitee.Services.Metrics
{
	public class MetricsService
	{
		public void UpdateBasicChurn(List<int> countries, int year, int month, IConfigurationRoot config)
		{
			string ledgerConnString = config.GetConnectionString("LedgerConnString");
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
				using (var ctx = new RemiteeServicesMetricsContext())
				{
					ctx.BasicChurns.AddRange(parsedData);
					ctx.SaveChanges();
				}
			}

		}

		public void UpdateNewSenders(int year, int month, IConfigurationRoot config)
		{
			string ledgerConnString = config.GetConnectionString("LedgerConnString");
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
			using (var ctx = new RemiteeServicesMetricsContext())
			{
				ctx.NewSenders.AddRange(parsedData);
				ctx.SaveChanges();
			}


		}

		public void UpdateReceivers(int year, int month, IConfigurationRoot config)
		{
			string ledgerConnString = config.GetConnectionString("LedgerConnString");
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
			using (var ctx = new RemiteeServicesMetricsContext())
			{
				ctx.Receivers.AddRange(parsedData);
				ctx.SaveChanges();
			}


		}

		public void UpdateRegistrating(int year, int month, IConfigurationRoot config)
		{
			string ledgerConnString = config.GetConnectionString("LedgerConnString");
			string walletConnString = config.GetConnectionString("WalletConnString");
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
						  .GroupBy(x => new { countryName = x.Field<string>("CountryName"),
							  year = x.Field<int>("Year"),
							  month = x.Field<int>("Month"),
							  countryCode = x.Field<string>("CountryCode") });
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
			using (var ctx = new RemiteeServicesMetricsContext())
			{
				ctx.Registratings.AddRange(parsedData.ToList());
				ctx.SaveChanges();
			}


		}

		public void UpdateSenders(int year, int month, IConfigurationRoot config)
		{
			string ledgerConnString = config.GetConnectionString("LedgerConnString");
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
			using (var ctx = new RemiteeServicesMetricsContext())
			{
				ctx.Senders.AddRange(parsedData);
				ctx.SaveChanges();
			}


		}

		public void UpdateSendersBreakdown(List<int> countries, int year, int month, IConfigurationRoot config)
		{
			string ledgerConnString = config.GetConnectionString("LedgerConnString");
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
				using (var ctx = new RemiteeServicesMetricsContext())
				{
					ctx.SendersBreakdowns.AddRange(parsedData);
					ctx.SaveChanges();
				}
			}

		}

		public void UpdateCorredores(int year, int month)
		{
			var results = new List<Corredore>();
			var queryTransactionalBase = $@"select newid() as Id,
											year(x.Date) as Year, 
                                            month(x.Date) as Month,
                                            x.SourceCountryName,
                                            x.SourceCountryCode, 
                                            x.TargetCountryName,
                                            x.TargetCountryCode,
                                            count(*) as Count,
                                            sum(x.isREMITEE) as Count_Remitee,
                                            sum(x.isINBOUND) as Count_Inbound,
                                            sum(x.isTOPUP) as Count_TOPUP,
                                            sum(x.isMT) as Count_MT,
                                            sum(x.PplAmountREMITEE) as GTV_Remitee,
                                            sum(x.PplAmountINBOUND) as GTV_Inbound,
                                            sum(x.PplAmountREMITEE)+sum(x.PplAmountINBOUND) as GTV_Total,
                                            sum(x.PplAmountTOPUP) as GTV_TOPUP,
                                            sum(x.PplAmountMT) as GTV_MT,
                                            sum(x.AmountREMITEE) as NTV_Remitee,
                                            sum(x.AmountINBOUND) as NTV_Inbound,
                                            sum(x.AmountREMITEE)+sum(x.AmountINBOUND) as NTV_Total,
                                            sum(x.AmountTOPUP) as NTV_TOPUP,
                                            sum(x.AmountMT) as NTV_MT,
                                            sum(x.FeeREMITEE) as Fee_Remitee,
                                            sum(x.FeeINBOUND) as Fee_Inbound,
                                            sum(x.FeeTOTAL) as Fee_Total,
                                            sum(x.SpreadREMITEE) as Spread_Remitee,
                                            sum(x.SpreadINBOUND) as Spread_Inbound,
                                            sum(x.SpreadTOTAL) as Spread_Total



                                            from (select t.CreatedAt as Date,
		                                            t.SourceCountryName, 
		                                            t.SourceCountryCode,
		                                            t.TargetCountryName,
		                                            t.TargetCountryCode,
		                                            t.Status,
		                                            case t.CollectMethod
			                                            when 'TOPUP' then 1
			                                            else 0
		                                            end as isTOPUP,
		                                            case t.CollectMethod
			                                            when 'TOPUP' then 0
			                                            else 1
		                                            end as isMT,
		                                            case 
			                                            when t.Client='REMITEE'
			                                            then t.NetAmountUSD
			                                            else 0
			                                            end as PplAmountREMITEE,

		
		                                            case t.Client
			                                            when 'REMITEE'
			                                            then 0
			                                            else t.NetAmountUSD
			                                            end as PplAmountINBOUND,
		                                            0 as PplAmountTOPUP,
		                                            case 
			                                            when t.CollectMethod != 'TOPUP'
			                                            then t.NetAmountUSD
			                                            else 0
			                                            end as PplAmountMT,


		                                            case t.Client
			                                            when 'REMITEE'
			                                            then  case 
					                                            when t.SourceCurrency = 'ARS' and t.TargetCurrency='ARS'
					                                            then (t.NetAmountSC + t.FeeAmountSC)/t.ARSExchangeRate
					                                            when t.SourceCurrency = 'CLP' and t.TargetCurrency='ARS'
					                                            then (t.NetAmountSC + t.FeeAmountSC)/t.ExchangeRateSC
					                                            else ((t.NetAmountSC + t.FeeAmountSC)/t.ExchangeRateSC)*(1-t.SpreadRate)
				                                            end
			                                            else 0
			                                            end as AmountREMITEE,
		                                            case t.Client
			                                            when 'REMITEE'
			                                            then 0
			                                            else t.NetAmountUSD + t.FeeAmountUSD
			                                            end as AmountINBOUND,
		                                            0 as AmountTOPUP,
		                                            case 
			                                            when t.CollectMethod != 'TOPUP'
			                                            then case t.Client
					                                            when 'REMITEE'
					                                            then  case 
							                                            when t.SourceCurrency = 'ARS' and t.TargetCurrency='ARS'
							                                            then (t.NetAmountSC + t.FeeAmountSC)/t.ARSExchangeRate
							                                            when t.SourceCurrency = 'CLP' and t.TargetCurrency='ARS'
							                                            then (t.NetAmountSC + t.FeeAmountSC)/t.ExchangeRateSC
							                                            else ((t.NetAmountSC + t.FeeAmountSC)/t.ExchangeRateSC)*(1-t.SpreadRate)
						                                            end
					                                            else t.NetAmountUSD + t.FeeAmountUSD
				                                            end
			                                            else 0
			                                            end as AmountMT,
		                                            case t.Client
			                                            when 'REMITEE'
			                                            then 1
			                                            else 0
			                                            end as isREMITEE,
		                                            case t.Client
			                                            when 'REMITEE'
			                                            then 0
			                                            else 1
			                                            end as isINBOUND,
		                                            case t.Client
			                                            when 'REMITEE'
			                                            then t.FeeAmountUSD
			                                            else 0
			                                            end as FeeREMITEE,
		                                            case t.Client
			                                            when 'REMITEE'
			                                            then 0
			                                            else t.FeeAmountUSD
			                                            end as FeeINBOUND, 
		                                            t.FeeAmountUSD as FeeTOTAL,
		                                            t.SpreadAmountUSD as SpreadTOTAL,
		                                            case 
			                                            when t.Client='REMITEE' 
			                                            then t.SpreadAmountUSD
			                                            else 0
			                                            end as SpreadREMITEE,
		                                            case 
			                                            when t.Client='REMITEE' 
			                                            then 0
			                                            else t.SpreadAmountUSD
													end as SpreadINBOUND

		                                            from TransactionalBase t
		                                            where t.Source='MoneyTransfer' and t.status!='REVERSED'
	                                            ---- fin MoneyTransfer ----
	                                            union
	                                            ---- inicio Ledger ----
		                                            select t.CreatedAt as Date,
		                                            t.SourceCountryName, 
		                                            t.SourceCountryCode,
		                                            t.TargetCountryName,
		                                            t.TargetCountryCode,
		                                            t.Status,
		                                            case t.CollectMethod
			                                            when 'TOPUP' then 1
			                                            else 0
		                                            end as isTOPUP,
		                                            case t.CollectMethod
			                                            when 'TOPUP' then 0
			                                            else 1
		                                            end as isMT,
		                                            t.NetAmountUSD as PplAmountREMITEE,

		
		                                            0 as PplAmountINBOUND,
		                                            case t.CollectMethod
			                                            when 'TOPUP' then t.NetAmountUSD
			                                            else 0
			                                            end as PplAmountTOPUP,
		                                            case 
			                                            when t.CollectMethod != 'TOPUP'
			                                            then t.NetAmountUSD
			                                            else 0
			                                            end as PplAmountMT,
		                                            case 
			                                            when t.SourceCountryCode='ARG' and t.TargetCountryCode='ARG'
			                                            then t.GrossAmountSC/t.ARSExchangeRate
			                                            when t.SourceCountryCode='CHL' and t.TargetCountryCode='CHL'
			                                            then t.GrossAmountSC/t.CLPExchangeRate
			                                            else t.GrossAmountSC*t.ExchangeRateSC
			                                            end as AmountREMITEE,
		                                            0 as AmountINBOUND,
		                                            case t.CollectMethod
			                                            when 'TOPUP' then case 
								                                            when t.SourceCountryCode='ARG' and t.TargetCountryCode='ARG'
								                                            then t.GrossAmountSC/t.ARSExchangeRate
								                                            when t.SourceCountryCode='CHL' and t.TargetCountryCode='CHL'
								                                            then t.GrossAmountSC/t.CLPExchangeRate
								                                            else t.GrossAmountSC*t.ExchangeRateSC
							                                            end
			                                            else 0 end as AmountTOPUP,
		                                            case 
			                                            when t.CollectMethod != 'TOPUP'
			                                            then case 
					                                            when t.SourceCountryCode='ARG' and t.TargetCountryCode='ARG'
					                                            then t.GrossAmountSC/t.ARSExchangeRate
					                                            when t.SourceCountryCode='CHL' and t.TargetCountryCode='CHL'
					                                            then t.GrossAmountSC/t.CLPExchangeRate
					                                            else t.GrossAmountSC*t.ExchangeRateSC
				                                            end
			                                            else 0
			                                            end as AmountMT,
		                                            1 as isREMITEE,
		                                            0 as isINBOUND,
		                                            t.FeeAmountUSD as FeeREMITEE,
		                                            0 as FeeINBOUND, 
		                                            t.FeeAmountUSD as FeeTOTAL,
		                                            t.SpreadAmountUSD as SpreadTOTAL,
		                                            t.SpreadAmountUSD as SpreadREMITEE,
		                                            0 as SpreadINBOUND

		                                            from TransactionalBase t
		                                            where t.Source='Ledger' and t.status!='REVERSED'
	                                            ---- fin Ledger ----
	                                            union
	                                            ---- inicio Wallet ----
		                                            select t.CreatedAt as Date,
		                                            t.SourceCountryName, 
		                                            t.SourceCountryCode,
		                                            t.TargetCountryName,
		                                            t.TargetCountryCode,
		                                            t.Status,
		                                            1 as isTOPUP,
		                                            0 as isMT,
		                                            t.NetAmountUSD as PplAmountREMITEE,
		                                            0 as PplAmountINBOUND,
		                                            case t.CollectMethod
			                                            when 'TOPUP' then t.NetAmountUSD
			                                            else 0
			                                            end as PplAmountTOPUP,
		                                            0 as PplAmountMT,
		                                            case
			                                            when t.SourceCurrency='CLP'
			                                            then t.GrossAmountSC/t.CLPExchangeRate
			                                            when t.SourceCurrency='ARS'
			                                            then t.GrossAmountSC/t.ARSExchangeRate
			                                            else 0
		                                            end as AmountREMITEE,
		                                            0 as AmountINBOUND,
		                                            case
			                                            when t.SourceCurrency='CLP'
			                                            then t.GrossAmountSC/t.CLPExchangeRate
			                                            when t.SourceCurrency='ARS'
			                                            then t.GrossAmountSC/t.ARSExchangeRate
			                                            else 0
		                                            end as AmountTOPUP,
		                                            0 as AmountMT,
		                                            1 as isREMITEE,
		                                            0 as isINBOUND,
		                                            0 as FeeREMITEE,
		                                            0 as FeeINBOUND, 
		                                            0 as FeeTOTAL,
		                                            0 as SpreadTOTAL,
		                                            0 as SpreadREMITEE,
		                                            0 as SpreadINBOUND

		                                            from TransactionalBase t
		                                            where t.Source='Wallet' and t.status!='REVERSED'
                                            ) x
                                            where year(x.Date)=@year and month(x.Date)=@month
                                            group by year(x.Date), month(x.Date), x.SourceCountryName, x.SourceCountryCode, x.TargetCountryName, x.TargetCountryCode
                                            order by [year] desc, [month] desc";

			using (var ctx = new RemiteeServicesMetricsContext())
			{

				var corredores = (from t in ctx.TransactionalBases
								  where t.CreatedAt.Year == year && t.CreatedAt.Month == month
								  select new TransactionalBase(t, 1)).ToList()
							   .GroupBy(x => new
							   {
								   Year = x.CreatedAt.Year,
								   Month = x.CreatedAt.Month,
								   SourceCountryName = x.SourceCountryName,
								   SourceCountryCode = x.SourceCountryCode,
								   TargetCountryName = x.TargetCountryName,
								   TargetCountryCode = x.TargetCountryCode
							   });
				foreach (var key in corredores)
				{
					results.Add(new Corredore(key.Key.Year,
											key.Key.Month,
											key.Key.SourceCountryName ?? "",
											key.Key.SourceCountryCode ?? "",
											key.Key.TargetCountryName ?? "",
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
											key.Where(x => x.Client == "REMITEE").Sum(x => x.NetAmountUsd + x.FeeAmountUsd + x.Vatusd) ?? 0,
											key.Where(x => x.Client != "REMITEE").Sum(x => x.NetAmountUsd + x.FeeAmountUsd + x.Vatusd) ?? 0,
											key.Sum(x => x.NetAmountUsd + x.FeeAmountUsd + x.Vatusd) ?? 0,
											key.Where(x => x.CollectMethod == "TOPUP").Sum(x => x.NetAmountUsd + x.FeeAmountUsd + x.Vatusd) ?? 0,
											key.Where(x => x.CollectMethod != "TOPUP").Sum(x => x.NetAmountUsd + x.FeeAmountUsd + x.Vatusd) ?? 0,
											key.Where(x => x.Client == "REMITEE").Sum(x => x.FeeAmountUsd) ?? 0,
											key.Where(x => x.Client != "REMITEE").Sum(x => x.FeeAmountUsd) ?? 0,
											key.Sum(x => x.FeeAmountUsd),
											key.Where(x => x.Client == "REMITEE").Sum(x => x.SpreadAmountUsd) ?? 0,
											key.Where(x => x.Client != "REMITEE").Sum(x => x.SpreadAmountUsd) ?? 0,
											key.Sum(x => x.SpreadAmountUsd)
											));
				}
				ctx.Corredores.AddRange(results);
				ctx.SaveChanges();
			}




		}

		public void UpdateTransactionalBase(DateTime dateFrom, DateTime dateTo, IConfigurationRoot config)
		{
			var parsedData = new List<TransactionalBase>();
			var finalData = new List<TransactionalBase>();
			var dataLedger = new DataTable();
			var dataWallet = new DataTable();
			var dataMoneyTransfer = new DataTable();
			string ledgerConnString = config.GetConnectionString("LedgerConnString");
			string walletConnString = config.GetConnectionString("WalletConnString");
			string moneyTransferConnString = config.GetConnectionString("MoneyTransferConnString");


			var queryLedger = @"select t.Id as Id,
								t.Id as LedgerId,
								'Ledger' as Source,
								t.createdDate as CreatedAt,
								sc.name as SourceCountryName,
								sc.isoCode as SourceCountryCode,
								tc.Name as TargetCountryName,
								tc.isoCode as TargetCountryCode,
								case 
								when t.status=6 then 'COMPLETED'
								when t.status>=3 then 'SETTLED'
								when t.status<0 then 'REVERSED'
								end as Status,
								case
								when t.collectMethod=1 then 'CASH'
								when t.collectMethod=2 then 'MONEY TRANSFER'
								else 'TOPUP'
								end as CollectMethod,
								t.sourceCode as Client,
								concat(v.code,'-',case when t.collectMethod<3 then 1 else 0 end, '-',p.currency,'-',tc.name) as OBPartner,
								p.name as OBPartnerName,
								isnull(t.SourceCurrency,sc.currencyCode) as SourceCurrency,
								isnull(p.currency,tc.currencyCode) as TargetCurrency,
								t.amount/(1+t.sourceTransactionFee*(1+t.sourceTaxRate)) as NetAmountSC,
								t.RecipientAmount*targetToUSDExchangeRate as NetAmountUSD,
								t.amount as GrossAmountSC,
								1/nullif(t.sourceToUSDExchangeRate,0) as ExchangeRateSC,
								null as SpreadAmountUSD,
								t.sourceExchangeRateSpread as SpreadRate,
								t.sourceTransactionFee*t.amount/(1+t.sourceTransactionFee*(1+t.sourceTaxRate)) as FeeAmountSC,
								t.sourceTransactionFee*t.RecipientAmount*targetToUSDExchangeRate as FeeAmountUSD,
								t.sourceTransactionFee as FeeRate,
								t.sourceTaxRate*t.sourceTransactionFee*t.amount/(1+t.sourceTransactionFee*(1+t.sourceTaxRate)) as VATSC,
								t.sourceTaxRate*t.sourceTransactionFee*t.RecipientAmount*targetToUSDExchangeRate as VATUSD,
								t.sourceTaxRate as VATRate,
								t.RecipientAmount as TargetAmountTC,
								1/nullif(t.targetToUSDExchangeRate,0) as ExchangeRateTC,
								null as MarketExchangeRate,
								p.code as PayerRoute,
								v.code as Payer,
								null as ARSExchangeRate,
								null as CLPExchangeRate,
								null as ARSRExchangeRate,
								null as CLPRExchangeRate,
								t.RecipientAmount as TargetAmountTCwithoutWithholding,
								null as WithholdingIncomeAmount,
								null as WithholdingVATAmount,
								null as WithholdingIncomeRate,
								null as WithholdingVATRate,
								t.RecipientAmount*(1+t.sourceTransactionFee*(1+t.sourceTaxRate))/nullif(t.amount,0) as 'AccountingFxRate',
								t.RecipientAmount*(1+t.sourceTransactionFee*(1+t.sourceTaxRate))/(nullif(t.amount,0)*(1-t.sourceExchangeRateSpread)) as 'AccountingFxRateWithoutSp',
								t.amount*(1-t.sourceExchangeRateSpread)/(1+t.sourceTransactionFee*(1+t.sourceTaxRate)) as 'AccountingNetAmount',
								t.amount*t.sourceExchangeRateSpread/(1+t.sourceTransactionFee*(1+t.sourceTaxRate)) as 'TargetSpreadSC',
								t.collectAgentCommission as AccountingAgentCommission,
								t.PayedDate as SettledAt,
								t.CollectedDate as CompletedAt,
								t.ReversedDate as ReversedAt,
								case
									  when v.Code = 'cienxcienbanco' then 'CIEN X CIEN BANCO'
									  when p.code like '%movii%' then 'MOVII'
									  when v.Code = 'transferzero' and p.code like '%EUR%' then 'TRANSFERZERO EUR'
									  when v.Code = 'transferzero' and p.code not like '%EUR%' then 'TRANSFERZERO USD'
									  when v.Code = 'localpayment' and p.code like '%BRL%' then 'LOCAL PAY. Brazil'
									  when v.Code = 'localpayment' and p.code like '%MXN%' then 'LOCAL PAY. Mexico'
									  when v.Code = 'localpayment' and p.code like '%UYU%' then 'LOCAL PAY. Uruguay'
									  when v.Code = 'localpayment' and p.code like '%COP%' then 'LOCAL PAY. Colombia'
									  when v.Code = 'interbank' and p.code like '%PEN%' then 'INTERBANK Soles'
									  when v.Code = 'interbank' and p.code like '%USD%' then 'INTERBANK USD'
									  when v.Code = 'EASYPAGOS' then 'EASY PAGOS'
									  when v.Code = 'bancobisa' then 'BANCO BISA'
									  when v.Code = 'bancosol' then 'BANCO SOLIDARIO'
									  when v.Code = 'maxicambios' then 'MAXICAMBIO Outbound'
									  when p.code like '%sogebank%' then 'SOGEBANK'
									  when p.code like '%b89%' then 'B89'
									  when p.code like '%kyodai%' then 'KYODAI'
									  when v.Code = 'pontual' then 'PONTUAL Outbound'
									  when p.code like '%pontual%' then 'PONTUAL Outbound'
									  when p.code is null and t.statusMessage = 'Transaction successful' then 'DTONE'
									  when p.code is null and t.statusMessage != 'Transaction successful' then 'THUNES'
									  else v.Code
								end as Vendor,
								null as ReferenceId,
								null as MarketPlaceFeeAmount,
								null as MarketPlaceFeeRate,
								null as MarketPlaceVATAmount,
								null as MarketPlaceVATRate,
								t.payerTransactionCode as PayerReferenceId,
								null as MarketPlaceExchangeRate,
								null as MarketPlaceFeeAmountUsd,
								null as MarketPlaceVatAmountUsd

								from Transactions t
								inner join Countries sc on sc.id = t.sourceCountryId
								inner join Countries tc on tc.id = t.targetCountryId
								left join Payers p on p.id=t.payerId
								left join Vendors v on v.id=t.vendorId
								where t.sourceType=0 and t.createdDate>=@dateFrom and t.createdDate<@dateTo
								and abs(t.status)>=3

                        ";
			var queryMoneyTransfer = @"select p.Id,
										p.transactionId as LedgerId,
										'MoneyTransfer' as Source,
										p.CreatedAt,
										null as SourceCountryName,
										case
											when c.Code='PAYRETAILERS'
											then 'ESP'
											when c.Code = 'PREXCARD'
											then 'URY'
											else isnull(p.BillingInfo_Ctry,p.UserInfo_Dbtr_PstlAdr_Ctry) 
											end as SourceCountryCode,
										null as TargetCountryName,
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
										qe.SendingAmount-qe.SendingFee-isnull(qe.SellerVATRemitee+qe.SellerVATMarketPlace,0) as NetAmountUSD,
										isnull(p.BillingInfo_NetAmount+p.BillingInfo_Commission_Amount+p.BillingInfo_Vat_Amount,qe.SendingAmount) as GrossAmountSC,
										isnull(p.BillingInfo_Fx_Rate,qe.SourceToUSD) as ExchangeRateSC,
										null as SpreadAmountUSD,
										isnull(p.BillingInfo_Fx_Spread,qe.FxSpread) as SpreadRate,
										case
										when qe.MarketPlaceBaseFee>0
										then round((greatest(qe.RemiteeBaseFee,qe.RemiteePercentFee)+greatest(qe.MarketPlaceBaseFee,qe.MarketPlacePercentFee))/nullif(qe.sendingAmount,0),4)*qe.SellerFx*qe.SendingAmount/(2*(1-isnull(qe.SellerFxSpread,0)))
										else isnull(p.BillingInfo_Commission_Amount,qe.sendingFee)
										end as FeeAmountSC,
										isnull(nullif(greatest(qe.RemiteeBaseFee,qe.RemiteePercentFee),0),qe.sendingFee) as FeeAmountUSD,
										isnull(p.BillingInfo_Commission_Rate,qe.SendingFee/(qe.SendingAmount-qe.SendingFee)) as FeeRate,
										case
										when qe.MarketPlaceBaseFee>0
										then round((greatest(qe.RemiteeBaseFee,qe.RemiteePercentFee)+greatest(qe.MarketPlaceBaseFee,qe.MarketPlacePercentFee))/nullif(qe.sendingAmount,0),4)*qe.SellerFx*qe.SendingAmount*qe.SellerVatPercent/(2*(1-isnull(qe.SellerFxSpread,0)))
										else isnull(p.BillingInfo_Vat_Amount,qe.SellerVatRemitee)
										end as VATSC,
										qe.SellerVATRemitee as VATUSD,
										isnull(p.BillingInfo_Vat_Rate,qe.SellerVATRemitee/nullif(greatest(qe.RemiteeBaseFee,qe.RemiteePercentFee),0)) as VATRate,
										qe.ReceivingAmount as TargetAmountTC,
										qe.FxRate as ExchangeRateTC,
										qe.MarketExchangeRate,
										py.Code as PayerRoute,
										py.VendorCode as Payer,
										null as ARSExchangeRate,
										null as CLPExchangeRate,
										null as ARSRExchangeRate,
										null as CLPRExchangeRate,
										qe.ReceivingAmount*(1-isnull(p.WithholdingPercentages_IncomeWithholdingPercentage,0)-isnull(p.WithholdingPercentages_VatWithholdingPercentage,0)) as TargetAmountTCwithoutWithholding,
										p.WithholdingPercentages_IncomeWithholdingPercentage*qe.ReceivingAmount as WithholdingIncomeAmount,
										p.WithholdingPercentages_VatWithholdingPercentage*qe.ReceivingAmount as WithholdingVATAmount,
										p.WithholdingPercentages_IncomeWithholdingPercentage as WithholdingIncomeRate,
										p.WithholdingPercentages_VatWithholdingPercentage as WithholdingVATRate,
										qe.ReceivingAmount/nullif(isnull(p.BillingInfo_NetAmount,qe.SendingAmount-qe.SendingFee-isnull(qe.SellerVATRemitee+qe.SellerVATMarketPlace,0)),0) as 'AccountingFxRate',
										qe.ReceivingAmount/(nullif(isnull(p.BillingInfo_NetAmount,qe.SendingAmount-qe.SendingFee-isnull(qe.SellerVATRemitee+qe.SellerVATMarketPlace,0)),0)*(1-qe.FxSpread)) as 'AccountingFxRateWithoutSp',
										(isnull(p.BillingInfo_NetAmount,qe.SendingAmount-qe.SendingFee-isnull(qe.SellerVATRemitee+qe.SellerVATMarketPlace,0))*(1-qe.FxSpread)) as 'AccountingNetAmount',
										isnull(p.BillingInfo_NetAmount,qe.SendingAmount-qe.SendingFee-isnull(qe.SellerVATRemitee+qe.SellerVATMarketPlace,0))*isnull(p.BillingInfo_Fx_Spread,qe.FxSpread) as 'SpreadAmountSc',
										null as AccountingAgentCommission	,
										p.SettledAt,
										p.CompletedAt,
										p.ReversedAt,
										case
										  when py.vendorCode = 'cienxcienbanco' then 'CIEN X CIEN BANCO'
										  when py.code like '%movii%' then 'MOVII'
										  when py.vendorCode = 'transferzero' and py.code like '%EUR%' then 'TRANSFERZERO EUR'
										  when py.vendorCode = 'transferzero' and py.code not like '%EUR%' then 'TRANSFERZERO USD'
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
										p.ReferenceId,
										case 
										when qe.MarketPlaceBaseFee>0
										then round((greatest(qe.MarketPlaceBaseFee,qe.MarketPlacePercentFee)+greatest(qe.RemiteeBaseFee,qe.RemiteePercentFee))/nullif(qe.sendingAmount,0),4)*qe.SellerFx*qe.SendingAmount/(2*(1-isnull(qe.SellerFxSpread,0)))
										else 0
										end as MarketPlaceFeeAmount,
										case 
										when qe.MarketPlaceBaseFee>0
										then round((greatest(qe.MarketPlaceBaseFee,qe.MarketPlacePercentFee)+greatest(qe.RemiteeBaseFee,qe.RemiteePercentFee))/nullif(qe.sendingAmount,0),4)*qe.SendingAmount/(2*(1-isnull(qe.SellerFxSpread,0))*nullif(qe.amountUSD,0)) 
										else 0
										end as MarketPlaceFeeRate,
										case 
										when qe.MarketPlaceBaseFee>0
										then round((greatest(qe.MarketPlaceBaseFee,qe.MarketPlacePercentFee)+greatest(qe.RemiteeBaseFee,qe.RemiteePercentFee))/nullif(qe.sendingAmount,0),4)*qe.SellerFx*qe.SendingAmount*qe.SellerVatPercent/(2*(1-isnull(qe.SellerFxSpread,0)))
										else 0
										end as MarketPlaceVATAmount,
										qe.SellerVatPercent as MarketPlaceVATRate,
										p.payerPaymentCode as PayerReferenceId,
										qe.SellerFx/(1-isnull(qe.SellerFxSpread,0)) as MarketPlaceExchangeRate,
										greatest(qe.MarketPlaceBaseFee,qe.MarketPlacePercentFee) as MarketPlaceFeeAmountUsd,
										greatest(qe.MarketPlaceBaseFee,qe.MarketPlacePercentFee)*qe.SellerVatPercent as MarketPlaceVatAmountUsd

								from mt.Payments p
								left join mt.QuoteElements qe on qe.QuoteId=p.QuoteId
								left join mt.Quotes q on q.id=qe.QuoteId
								left join mt.Clients c on c.Id=q.ClientId
								left join mt.PayerRoutes py on py.Id=p.payerRouteId
								left join mt.Payers pa on pa.id=q.PayerId
								left join mt.Agreements a on a.ClientId=p.ClientId and a.PayerId=q.PayerId
								where p.Status in (3,4,5) and p.CreatedAt>=@dateFrom and p.CreatedAt<@dateTo
								--and qe.AmountUSD>0
                                ";
			var queryWallet = @"select o.Id as Id,
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
								null as SpreadAmountUSD,
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
								null as ARSExchangeRate,
								null as CLPExchangeRate,
								null as ARSRExchangeRate,
								null as CLPRExchangeRate,
								o.DestinationAmount as TargetAmountTCwithoutWithholding,
								null as WithholdingIncomeAmount,
								null as WothholdingVATAmount,
								null as WithholdingIncomeRate,
								null as WithholdingVATRate,
								o.DestinationAmount/o.SourceAmount as 'AccountingFxRate',
								o.DestinationAmount/(o.SourceAmount*(1-isnull(ps.ExchangeRateSpread,0))) as 'AccountingFxRateWithoutSp',
								o.SourceAmount*(1-isnull(ps.ExchangeRateSpread,0)) as 'AccountingNetAmount',
								o.SourceAmount*isnull(ps.ExchangeRateSpread,0) as 'SpreadAmountSC',
								null as AccountingAgentCommission,
								o.SubmissionDateDateUTC as SettledAt,
								o.CompletionDateUTC as CompletedAt,
								o.ReverseDate as ReversedAt,
								null as Vendor,
								null as ReferenceId,
								null as MarketPlaceFeeAmount,
								null as MarketPlaceFeeRate,
								null as MarketPlaceVATAmount,
								null as MarketPlaceVATRate,
								o.payerPaymentCode as PayerReferenceId,
								null as MarketPlaceExchangeRate,
								null as MarketPlaceFeeAmountUsd,
								null as MarketPlaceVatAmountUsd

								from wallet.Operations o
								left join wallet.OperationPriceSnapshots op on op.Id=o.PriceSnapshotId
								left join wallet.PaymentChannels pc on pc.id=o.PaymentChannelId
								left join wallet.PaymentChannelCurrencies pcc on pcc.PaymentChannelId=pc.Id
								left join wallet.Countries sc on sc.Id=o.SourceCountryId
								left join wallet.Countries tc on tc.Id=o.DestinationCountryId
								left join wallet.OperationPriceSnapshots ps on ps.id=o.PriceSnapshotId
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
				command.CommandText = queryWallet;
				command.Parameters.Add("@dateFrom", SqlDbType.Date).Value = dateFrom;
				command.Parameters.Add("@dateTo", SqlDbType.Date).Value = dateTo;
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
				command.Parameters.Add("@dateFrom", SqlDbType.Date).Value = dateFrom;
				command.Parameters.Add("@dateTo", SqlDbType.Date).Value = dateTo;
				SqlDataAdapter reader = new SqlDataAdapter(command);
				reader.Fill(dataLedger);

				connection.Close();
			}
			using (var connection = new SqlConnection(moneyTransferConnString))
			{
				connection.Open();
				SqlCommand command = new SqlCommand();
				command.Connection = connection;
				command.CommandText = queryMoneyTransfer;
				command.Parameters.Add("@dateFrom", SqlDbType.Date).Value = dateFrom;
				command.Parameters.Add("@dateTo", SqlDbType.Date).Value = dateTo;
				SqlDataAdapter reader = new SqlDataAdapter(command);
				reader.Fill(dataMoneyTransfer);

				connection.Close();
			}
			using (var ctx = new RemiteeServicesMetricsContext())
			{
				ctx.TransactionalBases.Where(x => x.CreatedAt >= dateFrom && x.CreatedAt < dateTo).ExecuteDelete();
				parsedData = (from dl in dataLedger.AsEnumerable()
							  select new TransactionalBase(dl))
						  .Union(from dmt in dataMoneyTransfer.AsEnumerable()
								 select new TransactionalBase(dmt))
						  .Union(from dw in dataWallet.AsEnumerable()
								 select new TransactionalBase(dw))
						  .ToList();

				ctx.TransactionalBases.AddRange(parsedData);
				ctx.SaveChanges();
				finalData = (from tb in ctx.TransactionalBases
							 where tb.CreatedAt >= dateFrom && tb.CreatedAt < dateTo
							 select new TransactionalBase(tb)).ToList();

			}

			
			
			using (var ctx = new RemiteeServicesMetricsContext())
			{
				finalData.AddPayersData(dateFrom, dateTo, config);
				ctx.TransactionalBases.UpdateRange(finalData);
				ctx.SaveChanges();
			}

			

		}
		
		public void UpdateInbound(int year, int month)
		{
			var results = new List<Inbound>();
			var queryTransactionalBase = $@"select Newid() as Id,
											year(x.Date) as Year, 
											month(x.Date) as Month,
											x.Client as Partner,
											x.SourceCountryName,
											x.SourceCountryCode, 
											x.TargetCountryName,
											x.TargetCountryCode,
											count(*) as Count,
											sum(x.isTOPUP) as CountTopup,
											sum(x.isMT) as CountMt,
											sum(x.PplAmountTOPUP)+sum(x.PplAmountMT) as GtvTotal,
											sum(x.PplAmountTOPUP) as GtvTopup,
											sum(x.PplAmountMT) as GtvMt,
											sum(x.AmountTOPUP)+sum(x.AmountMT) as NtvTotal,
											sum(x.AmountTOPUP) as NtvTopup,
											sum(x.AmountMT) as NtvMt,
											avg(nullif(x.PplAmountTOPUP+x.PplAmountMT,0)) as GtvAvg,
											sum(x.FeeTOTAL) as Fee,
											sum(x.SpreadTOTAL) as Spread,
											sum(x.VAT) as Vat

											from (select t.CreatedAt as Date,
													t.SourceCountryName, 
													t.SourceCountryCode,
													t.TargetCountryName,
													t.TargetCountryCode,
													t.Status,
													t.Client,
													case t.CollectMethod
														when 'TOPUP' then 1
														else 0
													end 
													as isTOPUP,
													case t.CollectMethod
														when 'TOPUP' then 0
														else 1
													end 
													as isMT,
													0 
													as PplAmountTOPUP,
													case 
														when t.CollectMethod != 'TOPUP'
														then case t.Client
																when 'REMITEE'
																then  case 
																		when t.SourceCurrency = 'ARS' and t.TargetCurrency='ARS'
																		then t.NetAmountSC/t.ARSExchangeRate
																		when t.SourceCurrency = 'CLP' and t.TargetCurrency='ARS'
																		then t.NetAmountSC/t.ExchangeRateSC
																		else (t.NetAmountSC/t.ExchangeRateSC)*(1-t.SpreadRate)
																	end
																else t.NetAmountUSD
																end
														else 0
														end 
													as PplAmountMT,
													0 
													as AmountTOPUP,
													case 
														when t.CollectMethod != 'TOPUP'
														then case t.Client
																when 'REMITEE'
																then  case 
																		when t.SourceCurrency = 'ARS' and t.TargetCurrency='ARS'
																		then (t.NetAmountSC + t.FeeAmountSC)/t.ARSExchangeRate
																		when t.SourceCurrency = 'CLP' and t.TargetCurrency='ARS'
																		then (t.NetAmountSC + t.FeeAmountSC)/t.ExchangeRateSC
																		else ((t.NetAmountSC + t.FeeAmountSC)/t.ExchangeRateSC)*(1-t.SpreadRate)
																	end
																else t.NetAmountUSD + t.FeeAmountUSD
															end
														else 0
														end 
													as AmountMT,
													case 
														when t.Client='REMITEE' and t.SourceCurrency = 'ARS'
														then t.FeeAmountSC/t.ARSExchangeRate
														when t.Client='REMITEE' and t.SourceCurrency = 'CLP'
														then t.FeeRate*t.NetAmountSC/t.ExchangeRateSC
														else t.FeeAmountUSD
														end 
													as FeeTOTAL,
													case 
														when t.Client='REMITEE' 
														then case
																when t.TargetCurrency='CLP' and t.SourceCurrency='ARS'
																then t.NetAmountSC/t.ARSExchangeRate-t.NetAmountUSD--Ver 1
																when t.TargetCurrency='ARS' and t.SourceCurrency='CLP'
																then t.NetAmountSC/t.ExchangeRateSC-t.TargetAmountTC/t.ARSExchangeRate--Ver 2
																else 0
																end
														when t.TargetCurrency = 'ARS'
														then t.NetAmountUSD - t.TargetAmountTC/t.ARSExchangeRate  --Ver 3
														when t.SpreadRate is null
														then 0
														else case
															when t.targetCurrency='VEF'
															then t.NetAmountUSD*t.SpreadRate
															else t.NetAmountUSD-t.TargetAmountTC/t.MarketExchangeRate
															end
														end 
													as SpreadTOTAL,
													case 
														when t.Client='REMITEE' and t.SourceCurrency = 'ARS'
														then t.VATSC/t.ARSExchangeRate
														when t.Client='REMITEE' and t.SourceCurrency = 'CLP'
														then t.VATSC/t.ExchangeRateSC
														else 0
														end 
													as VAT

													from TransactionalBase t
													where t.Source='MoneyTransfer' and t.status!='REVERSED'
												---- fin MoneyTransfer ----
												union
												---- inicio Ledger ----
													select t.CreatedAt as Date,
													t.SourceCountryName, 
													t.SourceCountryCode,
													t.TargetCountryName,
													t.TargetCountryCode,
													t.Client,
													t.Status,
													case t.CollectMethod
														when 'TOPUP' then 1
														else 0
													end 
													as isTOPUP,
													case t.CollectMethod
														when 'TOPUP' then 0
														else 1
													end 
													as isMT,
													case t.CollectMethod
														when 'TOPUP' then case 
																			when t.TargetCountryCode='ARG' 
																			then t.TargetAmountTC/t.ARSExchangeRate
																			when t.TargetCountryCode='CHL'
																			then t.TargetAmountTC/t.CLPExchangeRate
																			else t.ExchangeRateTC*t.TargetAmountTC
																		end
														else 0
														end 
													as PplAmountTOPUP,
													case 
														when t.CollectMethod != 'TOPUP'
														then case 
																when t.TargetCountryCode='ARG' 
																then t.TargetAmountTC/t.ARSExchangeRate
																when t.TargetCountryCode='CHL'
																then t.TargetAmountTC/t.CLPExchangeRate
																else t.ExchangeRateTC*t.TargetAmountTC
															end
														else 0
														end 
													as PplAmountMT,
													case t.CollectMethod
														when 'TOPUP' then case 
																			when t.SourceCountryCode='ARG' and t.TargetCountryCode='ARG'
																			then t.GrossAmountSC/t.ARSExchangeRate
																			when t.SourceCountryCode='CHL' and t.TargetCountryCode='CHL'
																			then t.GrossAmountSC/t.CLPExchangeRate
																			else t.GrossAmountSC*t.ExchangeRateSC
																		end
														else 0 end 
													as AmountTOPUP,
													case 
														when t.CollectMethod != 'TOPUP'
														then case 
																when t.SourceCountryCode='ARG' and t.TargetCountryCode='ARG'
																then t.GrossAmountSC/t.ARSExchangeRate
																when t.SourceCountryCode='CHL' and t.TargetCountryCode='CHL'
																then t.GrossAmountSC/t.CLPExchangeRate
																else t.GrossAmountSC*t.ExchangeRateSC
															end
														else 0
														end 
													as AmountMT,
													case 
														when t.TargetCountryCode='ARG'
														then t.TargetAmountTC*t.FeeRate/t.ARSExchangeRate
														when t.TargetCountryCode='CHL'
														then t.TargetAmountTC*t.FeeRate/t.CLPExchangeRate
														else t.TargetAmountTC*t.FeeRate*t.ExchangeRateTC
														end 
													as FeeTOTAL,
													case 
													when t.SourceCountryCode='ARG' 
													then case t.TargetCountryCode
															when 'ARG'
															then t.GrossAmountSC*(1-t.FeeRate)/t.ARSExchangeRate-t.TargetAmountTC/t.ARSExchangeRate
															when 'CHL'
															then t.GrossAmountSC*(1-t.FeeRate)/t.ARSExchangeRate-t.TargetAmountTC/t.CLPExchangeRate
															else t.GrossAmountSC*(1-t.FeeRate)/t.ARSExchangeRate-t.TargetAmountTC/t.ExchangeRateTC
														 end
													when t.SourceCountryCode='CHL' 
													then case t.TargetCountryCode
															when 'ARG'
															then t.GrossAmountSC*(1-t.FeeRate)/t.CLPExchangeRate-t.TargetAmountTC/t.ARSExchangeRate
															when 'CHL'
															then t.GrossAmountSC*(1-t.FeeRate)/t.CLPExchangeRate-t.TargetAmountTC/t.CLPExchangeRate
															else t.GrossAmountSC*(1-t.FeeRate)/t.CLPExchangeRate-t.TargetAmountTC/t.ExchangeRateTC
														 end
													else t.NetAmountSC*t.ExchangeRateSC-t.TargetAmountTC*t.ExchangeRateTC
													end
													as SpreadTOTAL,
													case 
														when t.SourceCountryCode='ARG'
														then t.VATSC/t.ARSExchangeRate
														when t.SourceCountryCode='CHL'
														then t.VATSC/t.CLPExchangeRate
														else t.VATUSD
													end 
													as VAT
		

													from TransactionalBase t
													where t.Source='Ledger' and t.status!='REVERSED'
												---- fin Ledger ----
												union
												---- inicio Wallet ----
													select t.CreatedAt as Date,
													t.SourceCountryName, 
													t.SourceCountryCode,
													t.TargetCountryName,
													t.TargetCountryCode,
													t.Status,
													t.Client,
													1 as isTOPUP,
													0 as isMT,
													case t.CollectMethod
														when 'TOPUP' then case
																			when t.SourceCurrency='CLP'
																			then t.NetAmountSC/t.CLPExchangeRate
																			when t.SourceCurrency='ARS'
																			then t.NetAmountSC/t.ARSExchangeRate
																			else 0
																		end
														else 0
														end 
													as PplAmountTOPUP,
													0 
													as PplAmountMT,
													case
														when t.SourceCurrency='CLP'
														then t.GrossAmountSC/t.CLPExchangeRate
														when t.SourceCurrency='ARS'
														then t.GrossAmountSC/t.ARSExchangeRate
														else 0
													end 
													as AmountTOPUP,
													0 
													as AmountMT,
													0 
													as FeeTOTAL,
													0 
													as SpreadTOTAL,
													case
														when t.SourceCurrency='CLP'
														then 0
														when t.SourceCurrency='ARS'
														then t.VATSC/t.ARSExchangeRate
														else 0
													end 
													as VAT

													from TransactionalBase t
													where t.Source='Wallet' and t.status!='REVERSED'
											) x
											where year(x.Date)=@year and month(x.Date)=@month
											group by year(x.Date), month(x.Date), x.SourceCountryName, x.SourceCountryCode, x.TargetCountryName, x.TargetCountryCode, x.Client
											order by [year] desc, [month] desc";


			using (var ctx = new RemiteeServicesMetricsContext())
			{

				var inbound = (from t in ctx.TransactionalBases
							   where t.CreatedAt.Year == year && t.CreatedAt.Month == month
							   select new TransactionalBase(t, 1)).ToList()
							   .GroupBy(x => new
							   {
								   Year = x.CreatedAt.Year,
								   Month = x.CreatedAt.Month,
								   Partner = x.Client,
								   SourceCountryName = x.SourceCountryName,
								   SourceCountryCode = x.SourceCountryCode,
								   TargetCountryName = x.TargetCountryName,
								   TargetCountryCode = x.TargetCountryCode
							   });
				foreach (var key in inbound)
				{
					results.Add(new Inbound(key.Key.Year,
											key.Key.Month,
											key.Key.Partner ?? "REMITEE",
											key.Key.SourceCountryName ?? "",
											key.Key.SourceCountryCode ?? "",
											key.Key.TargetCountryName ?? "",
											key.Key.TargetCountryCode ?? "",
											key.Count(),
											key.Where(x => x.CollectMethod == "TOPUP").Count(),
											key.Where(x => x.CollectMethod != "TOPUP").Count(),
											key.Sum(x => x.NetAmountUsd) ?? 0,
											key.Where(x => x.CollectMethod == "TOPUP").Sum(x => x.NetAmountUsd) ?? 0,
											key.Where(x => x.CollectMethod != "TOPUP").Sum(x => x.NetAmountUsd) ?? 0,
											key.Sum(x => x.NetAmountUsd + x.FeeAmountUsd + x.Vatusd) ?? 0,
											key.Where(x => x.CollectMethod == "TOPUP").Sum(x => x.NetAmountUsd + x.FeeAmountUsd + x.Vatusd) ?? 0,
											key.Where(x => x.CollectMethod != "TOPUP").Sum(x => x.NetAmountUsd + x.FeeAmountUsd + x.Vatusd) ?? 0,
											key.Average(x => x.NetAmountUsd),
											key.Sum(x => x.FeeAmountUsd),
											key.Sum(x => x.SpreadAmountUsd),
											key.Sum(x => x.Vatusd)));
				}
				ctx.Inbounds.AddRange(results);
				ctx.SaveChanges();
			}
		}

		public void UpdateComplexChurn(int year, int month)
		{
			var records = new List<ComplexChurn>();

			using (var ctx = new RemiteeServicesMetricsContext())
			{
				
				ctx.Database.SetCommandTimeout(new TimeSpan(0, 5, 0));
				ctx.ComplexChurns.Where(x => x.CurrentMonth == month && x.CurrentYear == year).ExecuteDelete();

				var temp = (from s in ctx.Tcsenders
							 join t in ctx.Tctransactions on s.AccountId equals t.SenderId
							 join ft in ctx.Tctransactions
														 .GroupBy(x => x.SenderId)
														 .Select(x => new { DateCreated = x.Min(y => y.DateCreated), SenderId = x.Key }) 
														 on s.AccountId equals ft.SenderId
							 join tb in ctx.TransactionalBases on t.TrxReference equals tb.LedgerId.ToString()
							 where t.DateCreated.Year == year && t.DateCreated.Month == month
							 select new
							 {
								 SenderId = t.SenderId,
								 SourceCountryName = tb.SourceCountryName,
								 SourceCountryCode = tb.SourceCountryCode,
								 TargetCountryName = tb.TargetCountryName,
								 TargetCountryCode = tb.TargetCountryCode,
								 Year = t.TrxDate.Year,
								 Month = t.TrxDate.Month,
								 firstTrxMonth = ft.DateCreated.Month,
								 firstTrxYear = ft.DateCreated.Year,
								 Client = t.SendingClerkOrBranchId,
								 Status = tb.Status,
								 CollectMethod = tb.CollectMethod,
								 GTV = tb.NetAmountUsd
							 }).ToList();

				/*
				var test = (from t in ctx.Tctransactions.Where(x => x.DateCreated.Year == year && x.DateCreated.Month == month)
							join tb in ctx.TransactionalBases.Where(x => x.CreatedAt.Year == year && x.CreatedAt.Month == month && x.Status != "REVERSED") on t.TrxReference equals tb.LedgerId.ToString()
							 join s in ctx.Tcsenders on t.SenderId equals s.AccountId

							 select new
							 {
								 SenderId = t.SenderId,
								 SourceCountryName = tb.SourceCountryName,
								 SourceCountryCode = tb.SourceCountryCode,
								 TargetCountryName = tb.TargetCountryName,
								 TargetCountryCode = tb.TargetCountryCode,
								 Year = t.TrxDate.Year,
								 Month = t.TrxDate.Month,
								 Client = t.SendingClerkOrBranchId,
								 Status = tb.Status,
								 CollectMethod = tb.CollectMethod,
								 GTV = tb.NetAmountUsd
							 }).ToList();
				*/
				var churn = temp.GroupBy(x => new
										{
											SenderId = x.SenderId,
											Client = x.Client,
											FirstTrxMonth = x.firstTrxMonth,
											FirstTrxYear = x.firstTrxYear,
											SourceCountryName = x.SourceCountryName,
											SourceCountryCode = x.SourceCountryCode,
											TargetCountryName = x.TargetCountryName,
											TargetCountryCode = x.TargetCountryCode
										}).ToList();
				foreach (var key in churn)
				{
					/*
					using (var ctx2 = new RemiteeServicesMetricsContext())
					{
						ctx.Database.SetCommandTimeout(new TimeSpan(0, 3, 0));
						firstTrxDate = ctx2.Tctransactions.Where(x => x.SenderId == key.Key.SenderId && x.SendingClerkOrBranchId == key.Key.Client && x.PaymentStatus == "COMPLETED")
							.OrderBy(x => x.DateCreated)
							.ToList()
							.First()
							.DateCreated;
						firstTrxMonth = firstTrxDate.Month;
						firstTrxYear = firstTrxDate.Year;
					}
					*/

					records.Add(new ComplexChurn("Sender",
								Guid.Parse(key.Key.SenderId),
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
			using (var ctx = new RemiteeServicesMetricsContext())
			{
				ctx.Database.SetCommandTimeout(new TimeSpan(0, 5, 0));
				var tempQ = (from r in ctx.Tcreceivers
							join t in ctx.Tctransactions on r.AccountId equals t.ReceiverId
							join ft in ctx.Tctransactions.GroupBy(x => x.ReceiverId)
														 .Select(x => new { DateCreated = x.Min(y => y.DateCreated), ReceiverId = x.Key })
														 on r.AccountId equals ft.ReceiverId
							 join tb in ctx.TransactionalBases on t.TrxReference equals tb.LedgerId.ToString()
							 where t.DateCreated.Year == year && t.DateCreated.Month == month
							 select new
							 {
								 ReceiverId = t.ReceiverId,
								 SourceCountryName = tb.SourceCountryName,
								 SourceCountryCode = tb.SourceCountryCode,
								 TargetCountryName = tb.TargetCountryName,
								 TargetCountryCode = tb.TargetCountryCode,
								 Year = t.TrxDate.Year,
								 Month = t.TrxDate.Month,
								 firstTrxMonth = ft.DateCreated.Month,
								 firstTrxYear = ft.DateCreated.Year,
								 Client = t.SendingClerkOrBranchId,
								 Status = tb.Status,
								 CollectMethod = tb.CollectMethod,
								 GTV = tb.NetAmountUsd
							 });
				var sql = tempQ.ToQueryString();
				var temp = tempQ.ToList();
				var churn = temp.GroupBy(x => new
										{
											ReceiverId = x.ReceiverId,
											Client = x.Client,
											FirstTrxMonth = x.firstTrxMonth,
											FirstTrxYear = x.firstTrxYear,
											SourceCountryName = x.SourceCountryName,
											SourceCountryCode = x.SourceCountryCode,
											TargetCountryName = x.TargetCountryName,
											TargetCountryCode = x.TargetCountryCode
										}).ToList();
				foreach (var key in churn)
				{

					records.Add(new ComplexChurn("Receiver",
								Guid.Parse(key.Key.ReceiverId),
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

		public void UpdateSpreadsheet(SpreadSheetConnector connTransactionalBase, 
			SpreadSheetConnector connUsers,
			SpreadSheetConnector connCorredores,
			SpreadSheetConnector connInbound,
			SpreadSheetConnector connComplexChurn,
			int year, int month, DateTime dateFrom, DateTime dateTo)
		{
			GetNewSenders(connUsers, year, month);
			GetTransactionalBase(connTransactionalBase, dateFrom, dateTo);
			GetBasicChurn(connUsers, year, month);
			GetComplexChurn(connComplexChurn, year, month);
			GetCorredores(connCorredores, year, month);
			GetInbound(connInbound, year, month);
			GetSenders(connUsers, year, month);
			GetSendersBreakdown(connUsers, year, month);
			GetReceivers(connUsers, year, month);
			GetRegistrating(connUsers, year, month);

		}

		private void GetNewSenders(SpreadSheetConnector conn, int year, int month)
		{
			var data = new List<IList<object>>();

			using (var ctx = new RemiteeServicesMetricsContext())
			{
				data = ctx.NewSenders.Where(x => x.Year == year && x.Month == month).CreateListOfLists(false);
				conn.AppendData(data, "NewSenders");
			}
		}

		private void GetRegistrating(SpreadSheetConnector conn, int year, int month)
		{
			var data = new List<IList<object>>();

			using (var ctx = new RemiteeServicesMetricsContext())
			{
				data = ctx.Registratings.Where(x => x.Year == year && x.Month == month).CreateListOfLists(false);
				conn.AppendData(data, "Registrating");
			}
		}

		private void GetReceivers(SpreadSheetConnector conn, int year, int month)
		{
			var data = new List<IList<object>>();

			using (var ctx = new RemiteeServicesMetricsContext())
			{
				data = ctx.Receivers.Where(x => x.Year == year && x.Month == month).CreateListOfLists(false);
				conn.AppendData(data, "Receivers");
			}
		}

		private void GetSendersBreakdown(SpreadSheetConnector conn, int year, int month)
		{
			var data = new List<IList<object>>();

			using (var ctx = new RemiteeServicesMetricsContext())
			{
				data = ctx.SendersBreakdowns.Where(x => x.Year == year && x.Month == month).CreateListOfLists(false);
				conn.AppendData(data, "SendersBreakdown");
			}
		}

		private void GetSenders(SpreadSheetConnector conn, int year, int month)
		{
			var data = new List<IList<object>>();

			using (var ctx = new RemiteeServicesMetricsContext())
			{
				data = ctx.Senders.Where(x => x.Year == year && x.Month == month).CreateListOfLists(false);
				conn.AppendData(data, "Senders");
			}
		}

		private void GetInbound(SpreadSheetConnector conn, int year, int month)
		{
			var data = new List<IList<object>>();

			using (var ctx = new RemiteeServicesMetricsContext())
			{
				data = ctx.Inbounds.Where(x => x.Year == year && x.Month == month).CreateListOfLists(false);
				conn.AppendData(data, "Inbound");
			}
		}

		private void GetCorredores(SpreadSheetConnector conn, int year, int month)
		{
			var data = new List<IList<object>>();

			using (var ctx = new RemiteeServicesMetricsContext())
			{
				data = ctx.Corredores.Where(x => x.Year == year && x.Month == month).CreateListOfLists(false);
				conn.AppendData(data, "Corredores");
			}
		}

		public void GetComplexChurn(SpreadSheetConnector conn, int year, int month)
		{
			var data = new List<IList<object>>();

			using (var ctx = new RemiteeServicesMetricsContext())
			{
				data = ctx.ComplexChurns.Where(x => x.CurrentYear == year && x.CurrentMonth == month).CreateListOfLists(false);
				conn.AppendData(data, "Complex Churn");
			}
		}

		private void GetBasicChurn(SpreadSheetConnector conn, int year, int month)
		{
			var data = new List<IList<object>>();

			using (var ctx = new RemiteeServicesMetricsContext())
			{
				data = ctx.BasicChurns.Where(x => x.Year == year && x.Month == month).CreateListOfLists(false);
				conn.AppendData(data, "Basic Churn");
			}
		}

		private void GetTransactionalBase(SpreadSheetConnector conn, DateTime dateFrom, DateTime dateTo)
		{
			var data = new List<IList<object>>();

			using (var ctx = new RemiteeServicesMetricsContext())
			{
				data = ctx.TransactionalBases.Where(x => x.CreatedAt >= dateFrom && x.CreatedAt < dateTo && x.Status != "REVERSED").CreateListOfLists(false);
				conn.AppendData(data, "TransactionalBase");
			}
		}

		public void UpdateSpreadsheet(SpreadSheetConnector conn)
		{
			GetNewSenders(conn);
			GetTransactionalBase(conn);
			GetBasicChurn(conn);
			GetComplexChurn(conn);
			GetCorredores(conn);
			GetInbound(conn);
			GetSenders(conn);
			GetSendersBreakdown(conn);
			GetReceivers(conn);
			GetRegistrating(conn);

		}

		private void GetNewSenders(SpreadSheetConnector conn)
		{
			var data = new List<IList<object>>();

			using (var ctx = new RemiteeServicesMetricsContext())
			{
				data = ctx.NewSenders.CreateListOfLists(true);
				conn.UpdateData(data, "NewSenders!A1");
			}
		}

		private void GetRegistrating(SpreadSheetConnector conn)
		{
			var data = new List<IList<object>>();

			using (var ctx = new RemiteeServicesMetricsContext())
			{
				data = ctx.Registratings.CreateListOfLists(true);
				conn.UpdateData(data, "Registrating!A1");
			}
		}

		private void GetReceivers(SpreadSheetConnector conn)
		{
			var data = new List<IList<object>>();

			using (var ctx = new RemiteeServicesMetricsContext())
			{
				data = ctx.Receivers.CreateListOfLists(true);
				conn.UpdateData(data, "Receivers!A1");
			}
		}

		private void GetSendersBreakdown(SpreadSheetConnector conn)
		{
			var data = new List<IList<object>>();

			using (var ctx = new RemiteeServicesMetricsContext())
			{
				data = ctx.SendersBreakdowns.CreateListOfLists(true);
				conn.UpdateData(data, "SendersBreakdown!A1");
			}
		}

		private void GetSenders(SpreadSheetConnector conn)
		{
			var data = new List<IList<object>>();

			using (var ctx = new RemiteeServicesMetricsContext())
			{
				data = ctx.Senders.CreateListOfLists(true);
				conn.UpdateData(data, "Senders!A1");
			}
		}

		private void GetInbound(SpreadSheetConnector conn)
		{
			var data = new List<IList<object>>();

			using (var ctx = new RemiteeServicesMetricsContext())
			{
				data = ctx.Inbounds.CreateListOfLists(true);
				conn.UpdateData(data, "Inbound!A1");
			}
		}

		private void GetCorredores(SpreadSheetConnector conn)
		{
			var data = new List<IList<object>>();

			using (var ctx = new RemiteeServicesMetricsContext())
			{
				data = ctx.Corredores.CreateListOfLists(true);
				conn.UpdateData(data, "Corredores!A1");
			}
		}

		private void GetComplexChurn(SpreadSheetConnector conn)
		{
			var data = new List<IList<object>>();

			using (var ctx = new RemiteeServicesMetricsContext())
			{
				data = ctx.ComplexChurns.CreateListOfLists(true);
				conn.UpdateData(data, "'Complex Churn'!A1");
			}
		}

		private void GetBasicChurn(SpreadSheetConnector conn)
		{
			var data = new List<IList<object>>();

			using (var ctx = new RemiteeServicesMetricsContext())
			{
				data = ctx.BasicChurns.CreateListOfLists(true);
				conn.UpdateData(data, "'Basic Churn'!A1");
			}
		}

		private void GetTransactionalBase(SpreadSheetConnector conn)
		{
			var data = new List<IList<object>>();

			using (var ctx = new RemiteeServicesMetricsContext())
			{
				data = ctx.TransactionalBases.CreateListOfLists(true);
				conn.UpdateData(data, "TransactionalBase!A1");
			}
		}

		public void UpdateTCTables(DateTime dateFrom)
		{
			UpdateTCCountries();

			UpdateTCSenders(dateFrom);

			UpdateTCReceivers(dateFrom);

			UpdateTCTransactions(dateFrom);
        }

        private void UpdateTCReceivers(DateTime dateFrom)
        {
			var config = new MapperConfiguration(cfg => cfg.CreateMap<ModelsTC.Receiver, Models.Tcreceiver>());

			var mapper = config.CreateMapper();

			var users = new List<ModelsTC.Receiver>();

			using (var ctx = new RemiteeServicesTransactionCollectorDbContext())
			{
				users = ctx.Receivers.Where(x => ctx.Transactions.Where(y => y.DateCreated >= dateFrom && y.ReceiverId == x.AccountId).FirstOrDefault() != null).ToList();
			}

			var toUpdate = mapper.Map<List<ModelsTC.Receiver>, List<Models.Tcreceiver>>(users);

			using (var ctx2 = new RemiteeServicesMetricsContext())
			{
				ctx2.Tcreceivers.UpdateRange(toUpdate);
				var entities = ctx2.ChangeTracker.Entries().Where(x => x.State == EntityState.Modified);
				foreach (var entity in entities)
				{
					if (ctx2.Tcreceivers.Where(x => x.Id == Convert.ToInt32(entity.Property("Id").CurrentValue.ToString())).FirstOrDefault() == null)
					{
						entity.State = EntityState.Added;
					}
				}
				ctx2.SaveChanges();

			}
		}

        private void UpdateTCSenders(DateTime dateFrom)
        {
			var config = new MapperConfiguration(cfg => cfg.CreateMap<ModelsTC.Sender, Models.Tcsender>());

			var mapper = config.CreateMapper();

			var users = new List<ModelsTC.Sender>();

			using (var ctx = new RemiteeServicesTransactionCollectorDbContext())
			{
				users = ctx.Senders.Where(x => ctx.Transactions.Where(y => y.DateCreated >= dateFrom && y.SenderId == x.AccountId).FirstOrDefault() != null).ToList();
			}

			var toUpdate = mapper.Map<List<ModelsTC.Sender>, List<Models.Tcsender>>(users);

			using (var ctx2 = new RemiteeServicesMetricsContext())
			{
				ctx2.Tcsenders.UpdateRange(toUpdate);
				var entities = ctx2.ChangeTracker.Entries().Where(x => x.State == EntityState.Modified);
				foreach (var entity in entities)
				{
					if (ctx2.Tcsenders.Where(x => x.Id == Convert.ToInt32(entity.Property("Id").CurrentValue.ToString())).FirstOrDefault() == null)
					{
						entity.State = EntityState.Added;
					}
				}
				ctx2.SaveChanges();

			}
		}

        private void UpdateTCCountries()
        {
			var config = new MapperConfiguration(cfg => cfg.CreateMap<ModelsTC.Country, Models.Tccountry>());

			var mapper = config.CreateMapper();

			var countries = new List<ModelsTC.Country>();

			using (var ctx = new RemiteeServicesTransactionCollectorDbContext())
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
            using (var connection = new SqlConnection("Server=tcp:remiteesql.database.windows.net,1433;Database=Remitee;User Id=franco;Password=yCgmgQA8BFxcTGgZ;MultipleActiveResultSets=True;"))
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
			using (var connection = new SqlConnection("Server=tcp:remiteesql.database.windows.net,1433;Database=Remitee.Services.WalletDb;User Id=franco;Password=yCgmgQA8BFxcTGgZ;MultipleActiveResultSets=True;"))
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

			var toUpdate = mapper.Map<List<ModelsTC.Country>, List<Models.Tccountry>>(countries);
			toUpdate.AddRange(parsedLedgerData);
			toUpdate.AddRange(parsedWalletData);
			var unique = toUpdate.DistinctBy(x => x.Id);

			using (var ctx2 = new RemiteeServicesMetricsContext())
			{
				ctx2.Tccountries.UpdateRange(unique);
				var entities = ctx2.ChangeTracker.Entries().Where(x => x.State == EntityState.Modified);
				foreach (var entity in entities)
				{
					if (ctx2.Tccountries.Where(x => x.Id == entity.Property("Id").CurrentValue.ToString()).FirstOrDefault() == null)
					{
						entity.State = EntityState.Added;
					}
					else
                    {
						entity.State = EntityState.Detached;
                    }
				}
				ctx2.SaveChanges();

			}
		}

        public void UpdateTCTransactions(DateTime dateFrom)
		{
			var config = new MapperConfiguration(cfg => cfg.CreateMap<ModelsTC.Transaction, Models.Tctransaction>());

			var mapper = config.CreateMapper();

			var users = new List<ModelsTC.Transaction>();

			using (var ctx = new RemiteeServicesTransactionCollectorDbContext())
			{
				users = ctx.Transactions.Where(x => x.DateCreated >= dateFrom).ToList();
			}

			var toUpdate = mapper.Map<List<ModelsTC.Transaction>, List<Models.Tctransaction>>(users);

			using (var ctx2 = new RemiteeServicesMetricsContext())
			{
				ctx2.Tctransactions.UpdateRange(toUpdate);
				var entities = ctx2.ChangeTracker.Entries().Where(x => x.State == EntityState.Modified);
				foreach (var entity in entities)
				{
					if (ctx2.Tctransactions.Where(x => x.Id == Convert.ToInt32(entity.Property("Id").CurrentValue.ToString())).FirstOrDefault() == null)
					{
						entity.State = EntityState.Added;
					}
				}
				ctx2.SaveChanges();

			}
		}
		
		public async void ReadExchangeRates(SpreadSheetConnector conn, DateTime dateFrom, DateTime dateTo)
        {
			var data = conn.ReadData("Comitentes!E2:NE2", conn.fileLocation);
			data.AddRange(conn.ReadData("Comitentes!E42:NE42", conn.fileLocation));
			var parsedData = data.ParseExchangeRatesArs();
			var filteredData = parsedData.Where(x => x.Date >= dateFrom && x.Date < dateTo);
			using(var ctx = new RemiteeServicesMetricsContext())
            {
				ctx.ExchangeRates.Where(x => x.CountryCode == "ARG" && x.Date >= dateFrom && x.Date < dateTo).ExecuteDelete();
				ctx.ExchangeRates.AddRange(filteredData);
				var entities = ctx.ChangeTracker.Entries().Where(x => x.State == EntityState.Added);
				foreach(var entity in entities)
                {
					if(Convert.ToDecimal(entity.Property("ExchangeRate1").CurrentValue) == Convert.ToDecimal(0))
                    {
						if(DateTime.Parse(entity.Property("Date").CurrentValue.ToString()).Date == dateFrom)
                        {
							entity.Property("ExchangeRate1").CurrentValue
							= ctx.ExchangeRates
							.Where(x => x.Date <= DateTime.Parse(entity.Property("Date").CurrentValue.ToString()).Date && x.CountryCode == "ARG")
							.OrderByDescending(x => x.Date)
							.First().ExchangeRate1;
						}
                        else
                        {
							var test1 = entities
							.Where(x => DateTime.Parse(x.Property("Date").CurrentValue.ToString()).Date <= DateTime.Parse(entity.Property("Date").CurrentValue.ToString()).Date 
							&& x.Property("CountryCode").CurrentValue.ToString() == "ARG"
							&& (decimal) x.Property("ExchangeRate1").CurrentValue > Convert.ToDecimal(0))
							.OrderByDescending(x => DateTime.Parse(x.Property("Date").CurrentValue.ToString()).Date)
							.First().Property("ExchangeRate1").CurrentValue;
							
							var test = (decimal) test1;
							
							entity.Property("ExchangeRate1").CurrentValue = test;
						}
						
					}
                }
				ctx.SaveChanges();
			}
			
			var response = GetCurrencyLayerExchangeRates(dateFrom, dateTo);
			response.Wait();
			var result = JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(response.Result)["quotes"];
			var list = JsonConvert.DeserializeObject<Dictionary<string,Dictionary<string,string>>>(Convert.ToString(result));
			var toUpdate = new List<ExchangeRate>();
			foreach(var item in list.Keys)
            {
				var x = list[item]["USDCLP"];
				toUpdate.Add(new ExchangeRate("Chile", "CHL", DateTime.Parse(item).Date, decimal.Parse(list[item]["USDCLP"], new CultureInfo("en-US")), "USD", "CLP"));
            }
			using (var ctx = new RemiteeServicesMetricsContext())
			{
				ctx.ExchangeRates.AddRange(toUpdate);
				
				ctx.SaveChanges();
			}
		}

		public async Task<string?> GetCurrencyLayerExchangeRates(DateTime dateFrom, DateTime dateTo)
        {
			string? response = null;
			using (var client = new HttpClient())
			{
				response = await client.GetStringAsync(@"https://api.currencylayer.com/timeframe?access_key=55c7ec2cabff26b43e8eb956868f5e5d&currencies=CLP&source=USD&start_date=" + dateFrom.ToString("yyyy-MM-dd") + "&end_date=" + dateTo.AddDays(-1).ToString("yyyy-MM-dd"));
			}
			return response;
		}

		public void SendRevenue(int year, int month)
        {
			var revenue = new List<TransactionalBase>();
			using ( var ctx = new RemiteeServicesMetricsContext())
            {
				revenue = ctx.TransactionalBases.Where(x => x.CreatedAt.Year == year && x.CreatedAt.Month == month).ToList();

            }

			SaveToCsv(revenue, @"C:\Users\FrancoZeppilli\Documents\Remitee\Innovacion\Metrics\Revenue - " + month.ToString().PadLeft(2, '0') + year.ToString() + " - decimalSeparatorDot.csv", new CultureInfo("en-US", false));
			SaveToCsv(revenue, @"C:\Users\FrancoZeppilli\Documents\Remitee\Innovacion\Metrics\Revenue - " + month.ToString().PadLeft(2, '0') + year.ToString() + " - decimalSeparatorComma.csv", new CultureInfo("es-ES", false));
			try
			{
				System.Net.Mail.MailMessage messageA = new System.Net.Mail.MailMessage();
				System.Net.Mail.MailMessage message = new System.Net.Mail.MailMessage();
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

				messageA.Attachments.Add(new System.Net.Mail.Attachment(@"C:\Users\FrancoZeppilli\Documents\Remitee\Innovacion\Metrics\Revenue - " + month.ToString().PadLeft(2, '0') + year.ToString() + " - decimalSeparatorDot.csv"));
				message.Attachments.Add(new System.Net.Mail.Attachment(@"C:\Users\FrancoZeppilli\Documents\Remitee\Innovacion\Metrics\Revenue - " + month.ToString().PadLeft(2, '0') + year.ToString() + " - decimalSeparatorComma.csv"));
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
			File.WriteAllLines(path, lines.ToArray(),Encoding.UTF8);

		}

		public async void ReadPartnersOperations(SpreadSheetConnector conn, DateTime dateFrom, DateTime dateTo)
		{
			Random rnd = new Random();

			//var interests = conn.ReadData("Comitentes!D3:NE3");

			var expenses = conn.ReadData("ARS!D2:NE2",conn.fileLocation);
			expenses.AddRange(conn.ReadData("ARS!D322:NE327",conn.fileLocation));
			expenses.AddRange(conn.ReadData("ARS!D329:NE329",conn.fileLocation));
			var parsedExpenses = expenses.ParseExpenses(rnd);
			var filteredExpenses = parsedExpenses.Where(x => x.Date >= dateFrom && x.Date < dateTo);
			using (var ctx = new RemiteeServicesMetricsContext())
			{
				ctx.PartnersOperations.Where(x => x.Date >= dateFrom && x.Date < dateTo).ExecuteDelete();
				ctx.PartnersOperations.AddRange(filteredExpenses);
				
				ctx.SaveChanges();
			}

			var exchanges = conn.ReadData("Comitentes!E2:NE2", conn.fileLocation);
			exchanges.AddRange(conn.ReadData("Comitentes!E5:NE5", conn.fileLocation));
			exchanges.AddRange(conn.ReadData("Comitentes!E11:NE11", conn.fileLocation));
			exchanges.AddRange(conn.ReadData("Comitentes!E20:NE20", conn.fileLocation));
			exchanges.AddRange(conn.ReadData("Comitentes!E26:NE26", conn.fileLocation));
			exchanges.AddRange(conn.ReadData("Comitentes!E32:NE32", conn.fileLocation));
			exchanges.AddRange(conn.ReadData("Comitentes!E38:NE38", conn.fileLocation));
			exchanges.AddRange(conn.ReadData("Comitentes!E48:NE48", conn.fileLocation));
			exchanges.AddRange(conn.ReadData("Comitentes!E49:NE49", conn.fileLocation));

			exchanges.AddRange(conn.ReadData("ARS!E260:NE260", conn.fileLocation));
			exchanges.AddRange(conn.ReadData("Comitentes!E43:NE43", conn.fileLocation));
			var parsedExchanges = exchanges.ParseExchanges(rnd);
			var filteredExchanges = parsedExchanges.Where(x => x.Date >= dateFrom && x.Date < dateTo);
			using (var ctx = new RemiteeServicesMetricsContext())
			{
				ctx.PartnersOperations.AddRange(filteredExchanges);

				ctx.SaveChanges();
			}
			/*
			var obPartners = conn.ReadData("PARTNERS!C2:NE2");
			obPartners.AddRange(conn.ReadData("PARTNERS!C467:NE829"));
			var parsedObPartners = obPartners.ParseObPartners();
			var filteredObPartners = parsedObPartners;
			//.Where(x => x.Date.Year == year && x.Date.Month == month);
			using (var ctx = new RemiteeServicesMetricsContext())
			{
				ctx.PartnersOperations.AddRange(filteredObPartners);

				ctx.SaveChanges();
			}
			*/
		}

		public void AddArsRExchangeRate(DateTime dateFrom, DateTime dateTo, IConfigurationRoot config)
        {
			
			var amount = Convert.ToDecimal(config.GetSection("AppSettings:ArsRExchangeRate:" + dateFrom.ToString("yyyy-MM-dd") + ":Balance").Value, CultureInfo.InvariantCulture);
			var exRate = Convert.ToDecimal(config.GetSection("AppSettings:ArsRExchangeRate:" + dateFrom.ToString("yyyy-MM-dd") + ":Rate").Value, CultureInfo.InvariantCulture);
			using (var ctx = new RemiteeServicesMetricsContext())
            {
				ctx.Database.SetCommandTimeout(new TimeSpan(0, 3, 0));
				var data = ctx.TransactionalBases
								   .Where(tb => tb.CreatedAt >= dateFrom
									   && tb.CreatedAt < dateTo
									   && tb.Status != "REVERSED"
									   && (tb.SourceCurrency == "ARS" || tb.TargetCurrency == "ARS"))
								   .Select(tb => new { Id = tb.Id,
									   CreatedAt = tb.CreatedAt,
									   Type = (
												tb.TargetCurrency == tb.SourceCurrency ? null :
												tb.SourceCurrency == "ARS" ? "CREDIT" : "DEBIT"
											  ),
									   Detalle = (string?)(tb.SourceCurrency + " - " + tb.TargetCurrency),
									   SourceCurrency = (string?)tb.SourceCurrency,
									   TargetCurrency = (string?)tb.TargetCurrency,
									   Amount = (
												   tb.TargetCurrency == "ARS" ? tb.TargetAmountTc : tb.GrossAmountSc
												),
									   ExRate = (
												   tb.TargetCurrency == tb.SourceCurrency ? null :
												   tb.SourceCurrency == "ARS" ? tb.GrossAmountSc / tb.NetAmountUsd :
												   tb.TargetAmountTc / (tb.GrossAmountSc * tb.ExchangeRateSc)
												) }).ToList();
				data.AddRange(ctx.PartnersOperations
								   .Where(po => po.Date >= dateFrom
										   && po.Date < dateTo
										   //&& po.Date.Month == month
										   && (po.SourceCurrency == "ARS" || po.TargetCurrency == "ARS"))
										   .Select(po => new
										   {
											   Id = po.Id.ToString(),
											   CreatedAt = po.Date,
											   Type = (string?)po.Type,
											   Detalle = (string?)po.Description,
											   SourceCurrency = (string?)po.SourceCurrency,
											   TargetCurrency = (string?)po.TargetCurrency,
											   Amount = (decimal?)po.Amount,
											   ExRate = po.ExchangeRate
										   }));
				data.Add(new
				{
					Id = "Beggining Balance",
					CreatedAt = dateFrom,
					Type = (string?)"CREDIT",
					Detalle = (string?)"Origin Credit",
					SourceCurrency = (string?)"ARS",
					TargetCurrency = (string?)null,
					Amount = (decimal?)amount,
					ExRate = (decimal?)exRate
				});
				var transactions = data.OrderBy(x => x.CreatedAt);

				decimal preBalance = 0;
				decimal postBalance = 0;
				decimal? rate = 0;

				for (int i = 0; i < transactions.Count(); i++)
                {
                    preBalance = postBalance;
					if(transactions.ElementAt(i).Type == "CREDIT")
                    {
						postBalance += transactions.ElementAt(i).Amount??0;
						rate = ( preBalance * rate + transactions.ElementAt(i).Amount * (transactions.ElementAt(i).Detalle == "Intereses" || transactions.ElementAt(i).Detalle == "Pago a Proveedores" ? rate : transactions.ElementAt(i).ExRate) ) / postBalance;
						var trx = ctx.TransactionalBases.Find(transactions.ElementAt(i).Id);
						if(trx != null)
                        {
							ctx.Entry(trx).Property(x => x.ArsrexchangeRate).CurrentValue = rate;
						}
						
					}
					else if(transactions.ElementAt(i).Type == "DEBIT")
                    {
						postBalance -= transactions.ElementAt(i).Amount ?? 0;
						
						var trx = ctx.TransactionalBases.Find(transactions.ElementAt(i).Id);
						if (trx != null)
						{
							ctx.Entry(trx).Property(x => x.ArsrexchangeRate).CurrentValue = rate;
						}
					}
                    else
                    {
						var trx = ctx.TransactionalBases.Find(transactions.ElementAt(i).Id);
						if (trx != null)
						{
							ctx.Entry(trx).Property(x => x.ArsrexchangeRate).CurrentValue = rate;
						}
					}
                }
				var settings = new SettingsController();
				SettingsController.AddOrUpdateAppSetting("AppSettings:ArsRExchangeRate:" + dateTo.ToString("yyyy-MM-dd") + ":Balance", postBalance);
				SettingsController.AddOrUpdateAppSetting("AppSettings:ArsRExchangeRate:" + dateTo.ToString("yyyy-MM-dd") + ":Rate", rate);
				ctx.SaveChanges();

            }

        }
	}
}
