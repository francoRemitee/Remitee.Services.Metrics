using System;
using System.Collections.Generic;
using System.Data;

namespace Remitee.Services.Metrics.Models;

public partial class TransactionalBase
{
    public string Id { get; set; } = null!;

    public int? LedgerId { get; set; }

    public string? Source { get; set; }

    public DateTime CreatedAt { get; set; }

    public string? SourceCountryName { get; set; }

    public string? SourceCountryCode { get; set; }

    public string? TargetCountryName { get; set; }

    public string? TargetCountryCode { get; set; }

    public string? Status { get; set; }

    public string? CollectMethod { get; set; }

    public string? Client { get; set; }

    public string? Obpartner { get; set; }

    public string? ObpartnerName { get; set; }

    public string? SourceCurrency { get; set; }

    public string? TargetCurrency { get; set; }

    public decimal? NetAmountSc { get; set; }

    public decimal? NetAmountUsd { get; set; }

    public decimal? GrossAmountSc { get; set; }

    public decimal? ExchangeRateSc { get; set; }

    public decimal? SpreadAmountUsd { get; set; }

    public decimal? SpreadRate { get; set; }

    public decimal? FeeAmountSc { get; set; }

    public decimal? FeeAmountUsd { get; set; }

    public decimal? FeeRate { get; set; }

    public decimal? Vatsc { get; set; }

    public decimal? Vatusd { get; set; }

    public decimal? Vatrate { get; set; }

    public decimal? TargetAmountTc { get; set; }

    public decimal? TargetAmountTcwithoutWithholding { get; set; }

    public decimal? WithholdingIncomeAmount { get; set; }

    public decimal? WithholdingVatAmount { get; set; }

    public decimal? WithholdingIncomeRate { get; set; }

    public decimal? WithholdingVatRate { get; set; }

    public decimal? ExchangeRateTc { get; set; }

    public decimal? MarketExchangeRate { get; set; }

    public string? PayerRoute { get; set; }

    public string? Payer { get; set; }

    public decimal? ArsexchangeRate { get; set; }

    public decimal? ClpexchangeRate { get; set; }

    public decimal? ArsrexchangeRate { get; set; }

    public decimal? ClprexchangeRate { get; set; }

    public decimal? AccountingFxRate { get; set; }

    public decimal? AccountingFxRateWithoutSp { get; set; }

    public decimal? AccountingNetAmount { get; set; }

    public decimal? SpreadAmountSc { get; set; }

    public decimal? AccountingAgentCommission { get; set; }

    public string? ClientReferenceId { get; set; }

    public decimal? MarketPlaceFeeAmount { get; set; }

    public decimal? MarketPlaceFeeRate { get; set; }

    public decimal? MarketPlaceVatAmount { get; set; }

    public decimal? MarketPlaceVatRate { get; set; }

    public string? Vendor { get; set; }

    public DateTime? SettledAt { get; set; }

    public DateTime? CompletedAt { get; set; }

    public DateTime? ReversedAt { get; set; }

    public string? PayerReferenceId { get; set; }

    public decimal? MarketPlaceExchangeRate { get; set; }

    public decimal? PayerExchangeRate { get; set; }

    public decimal? RemiteeCalculatedFee { get; set; }

    public decimal? PayerFee { get; set; }

    public decimal? PayerFeeExpected { get; set; }

    public decimal? PayerExchangeRateExpected { get; set; }

    public decimal? MarketPlaceFeeAmountUsd { get; set; }

    public decimal? MarketPlaceVatAmountUsd { get; set; }

    public decimal? ArsExchangeRateOf { get; set; }

    public long? BillingSiiFolio { get; set; }

    public decimal? BillingTotalAmount { get; set; }

    public decimal? BillingNetTaxed { get; set; }

    public decimal? BillingTaxAmount { get; set; }

    public string? BillingClientName { get; set; }

    public string? BillingClientDocumentNumber { get; set; }

    public DateTime? ForwardedAt { get; set; }

    public DateTime? LastPushedAt { get; set; }

    public DateTime? ForwardedAtFixed { get; set; }

    public DateTime? LastPushedAtFixed { get; set; }

    public DateTime? SettledAtFixed { get; set; }

    public DateTime? CompletedAtFixed { get; set; }

    public DateTime? ReversedAtFixed { get; set; }

    public DateTime? CreatedAtFixed { get; set; }

    public string? StatusFixed { get; set; }

    public decimal? ReferenceExchangeRate { get; set; }

    public decimal? IbSpreadRate { get; set; }

    public decimal? ObSpreadRate { get; set; }

    public decimal? ObFeeAmount { get; set; }

    public string? ObFeeCurrency { get; set; }

    public int? TransactionType { get; set; }

    public string? AccountingPeriod { get; set; }


    public virtual ICollection<FlatTransaction> FlatTransactions { get; } = new List<FlatTransaction>();

    public TransactionalBase()
    {

    }

    public TransactionalBase(TransactionalBase tb, int i)
    {
        Id = tb.Id;
        LedgerId = tb.LedgerId;
        Source = tb.Source;
        CreatedAt = tb.CreatedAt;
        SourceCountryName = tb.SourceCountryName;
        SourceCountryCode = tb.SourceCountryCode;
        TargetCountryName = tb.TargetCountryName;
        TargetCountryCode = tb.TargetCountryCode;
        Status = tb.Status;
        CollectMethod = tb.CollectMethod;
        Client = tb.Client;
        Obpartner = tb.Obpartner;
        SourceCurrency = tb.SourceCurrency;
        TargetCurrency = tb.TargetCurrency;
        NetAmountSc = tb.NetAmountSc;
        NetAmountUsd = tb.NetAmountUsd;
        GrossAmountSc = tb.GrossAmountSc;
        ExchangeRateSc = tb.ExchangeRateSc;
        SpreadRate = tb.SpreadRate;
        FeeAmountSc = tb.FeeAmountSc;
        FeeAmountUsd = tb.FeeAmountUsd;
        Vatusd = tb.Vatusd;
        FeeRate = tb.FeeRate;
        Vatsc = tb.Vatsc;
        Vatrate = tb.Vatrate;
        TargetAmountTc = tb.TargetAmountTc;
        TargetAmountTcwithoutWithholding = tb.TargetAmountTcwithoutWithholding;
        WithholdingIncomeAmount = tb.WithholdingIncomeAmount;
        WithholdingVatAmount = tb.WithholdingVatAmount;
        ExchangeRateTc = tb.ExchangeRateTc;
        MarketExchangeRate = tb.MarketExchangeRate;
        PayerRoute = tb.PayerRoute;
        Payer = tb.Payer;
        ArsexchangeRate = tb.ArsexchangeRate;
        ClpexchangeRate = tb.ClpexchangeRate;
        ArsrexchangeRate = tb.ArsrexchangeRate;
        ClprexchangeRate = tb.ClprexchangeRate;
        SpreadAmountUsd = tb.SpreadAmountUsd;
        ObpartnerName = tb.ObpartnerName;
        WithholdingIncomeRate = tb.WithholdingIncomeRate;
        WithholdingVatRate = tb.WithholdingVatRate;
        AccountingFxRate = tb.AccountingFxRate;
        AccountingFxRateWithoutSp = tb.AccountingFxRateWithoutSp;
        AccountingNetAmount = tb.AccountingNetAmount;
        SpreadAmountSc = tb.SpreadAmountSc;
        AccountingAgentCommission = tb.AccountingAgentCommission;
        SettledAt = tb.SettledAt;
        CompletedAt = tb.CompletedAt;
        ReversedAt = tb.ReversedAt;
        ClientReferenceId = tb.ClientReferenceId;
        MarketPlaceFeeAmount = tb.MarketPlaceFeeAmount;
        MarketPlaceFeeAmountUsd = tb.MarketPlaceFeeAmountUsd;
        MarketPlaceVatAmountUsd = tb.MarketPlaceVatAmountUsd;
        MarketPlaceFeeRate = tb.MarketPlaceFeeRate;
        MarketPlaceVatAmount = tb.MarketPlaceVatAmount;
        MarketPlaceVatRate = tb.MarketPlaceVatRate;
        Vendor = tb.Vendor;
        PayerReferenceId = tb.PayerReferenceId;
        MarketPlaceExchangeRate = tb.MarketPlaceExchangeRate;
        PayerExchangeRate = tb.PayerExchangeRate;
        RemiteeCalculatedFee = tb.RemiteeCalculatedFee;
        PayerFee = tb.PayerFee;
        PayerFeeExpected = tb.PayerFeeExpected;
        PayerExchangeRateExpected = tb.PayerExchangeRateExpected;
        ArsExchangeRateOf = tb.ArsExchangeRateOf;
        BillingClientDocumentNumber = tb.BillingClientDocumentNumber;
        BillingClientName = tb.BillingClientName;
        BillingNetTaxed = tb.BillingNetTaxed;
        BillingSiiFolio = tb.BillingSiiFolio;
        BillingTaxAmount = tb.BillingTaxAmount;
        BillingTotalAmount = tb.BillingTotalAmount;
        ForwardedAt = tb.ForwardedAt;
        LastPushedAt = tb.LastPushedAt;
        CreatedAtFixed = tb.CreatedAtFixed;
        SettledAtFixed = tb.SettledAtFixed;
        CompletedAtFixed = tb.CompletedAtFixed;
        ReversedAtFixed = tb.ReversedAtFixed;
        ForwardedAtFixed = tb.ForwardedAtFixed;
        LastPushedAtFixed = tb.LastPushedAtFixed;
        StatusFixed = tb.StatusFixed;
        ReferenceExchangeRate = tb.ReferenceExchangeRate;
        IbSpreadRate = tb.IbSpreadRate;
        ObSpreadRate = tb.ObSpreadRate;
        ObFeeAmount = tb.ObFeeAmount;
        ObFeeCurrency = tb.ObFeeCurrency;
        TransactionType = tb.TransactionType;
        AccountingPeriod = tb.AccountingPeriod;
    }

    public TransactionalBase(DataRow dtr)
    {
        Id = dtr.ItemArray[0].ToString();
        if (dtr.ItemArray[1] != DBNull.Value)
        {
            LedgerId = Convert.ToInt32(dtr.ItemArray[1]);
        }
        Source = dtr.ItemArray[2].ToString();
        CreatedAt = Convert.ToDateTime(dtr.ItemArray[3]);
        SourceCountryName = dtr.ItemArray[4].ToString();
        SourceCountryCode = dtr.ItemArray[5].ToString();
        TargetCountryName = dtr.ItemArray[6].ToString();
        TargetCountryCode = dtr.ItemArray[7]?.ToString();
        Status = dtr.ItemArray[8]?.ToString();
        CollectMethod = dtr.ItemArray[9]?.ToString();
        Client = dtr.ItemArray[10]?.ToString();
        Obpartner = dtr.ItemArray[11]?.ToString();
        ObpartnerName = dtr.ItemArray[12]?.ToString();
        SourceCurrency = dtr.ItemArray[13]?.ToString();
        TargetCurrency = dtr.ItemArray[14]?.ToString();
        if (dtr.ItemArray[15] != DBNull.Value)
        {
            NetAmountSc = Convert.ToDecimal(dtr.ItemArray[15]);
        }
        if (dtr.ItemArray[16] != DBNull.Value)
        {
            NetAmountUsd = Convert.ToDecimal(dtr.ItemArray[16]);
        }
        if (dtr.ItemArray[17] != DBNull.Value)
        {
            GrossAmountSc = Convert.ToDecimal(dtr.ItemArray[17]);
        }
        if (dtr.ItemArray[18] != DBNull.Value)
        {
            ExchangeRateSc = Convert.ToDecimal(dtr.ItemArray[18]);
        }
        if (dtr.ItemArray[19] != DBNull.Value)
        {
            SpreadAmountUsd = Convert.ToDecimal(dtr.ItemArray[19]);
        }
        if (dtr.ItemArray[20] != DBNull.Value)
        {
            SpreadRate = Convert.ToDecimal(dtr.ItemArray[20]);
        }
        if (dtr.ItemArray[21] != DBNull.Value)
        {
            FeeAmountSc = Convert.ToDecimal(dtr.ItemArray[21]);
        }
        if (dtr.ItemArray[22] != DBNull.Value)
        {
            FeeAmountUsd = Convert.ToDecimal(dtr.ItemArray[22]);
        }
        if (dtr.ItemArray[23] != DBNull.Value)
        {
            FeeRate = Convert.ToDecimal(dtr.ItemArray[23]);
        }
        if (dtr.ItemArray[24] != DBNull.Value)
        {
            Vatsc = Convert.ToDecimal(dtr.ItemArray[24]);
        }
        if (dtr.ItemArray[25] != DBNull.Value)
        {
            Vatusd = Convert.ToDecimal(dtr.ItemArray[25]);
        }
        if (dtr.ItemArray[26] != DBNull.Value)
        {
            Vatrate = Convert.ToDecimal(dtr.ItemArray[26]);
        }
        if (dtr.ItemArray[27] != DBNull.Value)
        {
            TargetAmountTc = Convert.ToDecimal(dtr.ItemArray[27]);
        }
        if (dtr.ItemArray[28] != DBNull.Value)
        {
            ExchangeRateTc = Convert.ToDecimal(dtr.ItemArray[28]);
        }
        if (dtr.ItemArray[29] != DBNull.Value)
        {
            MarketExchangeRate = Convert.ToDecimal(dtr.ItemArray[29]);
        }
        PayerRoute = dtr.ItemArray[30]?.ToString();
        Payer = dtr.ItemArray[31]?.ToString();
        ArsexchangeRate = null;
        ClpexchangeRate = null;
        if (dtr.ItemArray[34] != DBNull.Value)
        {
            ArsrexchangeRate = Convert.ToDecimal(dtr.ItemArray[34]);
        }
        if (dtr.ItemArray[35] != DBNull.Value)
        {
            ClprexchangeRate = Convert.ToDecimal(dtr.ItemArray[35]);
        }
        if (dtr.ItemArray[36] != DBNull.Value)
        {
            TargetAmountTcwithoutWithholding = Convert.ToDecimal(dtr.ItemArray[36]);
        }
        if (dtr.ItemArray[37] != DBNull.Value)
        {
            WithholdingIncomeAmount = Convert.ToDecimal(dtr.ItemArray[37]);
        }
        if (dtr.ItemArray[38] != DBNull.Value)
        {
            WithholdingVatAmount = Convert.ToDecimal(dtr.ItemArray[38]);
        }
        if (dtr.ItemArray[39] != DBNull.Value)
        {
            WithholdingIncomeRate = Convert.ToDecimal(dtr.ItemArray[39]);
        }
        if (dtr.ItemArray[40] != DBNull.Value)
        {
            WithholdingVatRate = Convert.ToDecimal(dtr.ItemArray[40]);
        }
        if (dtr.ItemArray[41] != DBNull.Value)
        {
            AccountingFxRate = Convert.ToDecimal(dtr.ItemArray[41]);
        }
        if (dtr.ItemArray[42] != DBNull.Value)
        {
            AccountingFxRateWithoutSp = Convert.ToDecimal(dtr.ItemArray[42]);
        }
        if (dtr.ItemArray[43] != DBNull.Value)
        {
            AccountingNetAmount = Convert.ToDecimal(dtr.ItemArray[43]);
        }
        if (dtr.ItemArray[44] != DBNull.Value)
        {
            SpreadAmountSc = Convert.ToDecimal(dtr.ItemArray[44]);
        }
        if (dtr.ItemArray[45] != DBNull.Value)
        {
            AccountingAgentCommission = Convert.ToDecimal(dtr.ItemArray[45]);
        }
        if (dtr.ItemArray[46] != DBNull.Value)
        {
            SettledAt = Convert.ToDateTime(dtr.ItemArray[46]);
        }
        if (dtr.ItemArray[47] != DBNull.Value)
        {
            CompletedAt = Convert.ToDateTime(dtr.ItemArray[47]);
        }
        if (dtr.ItemArray[48] != DBNull.Value)
        {
            ReversedAt = Convert.ToDateTime(dtr.ItemArray[48]);
        }
        if (dtr.ItemArray[49] != DBNull.Value)
        {
            Vendor = dtr.ItemArray[49].ToString();
        }
        if (dtr.ItemArray[50] != DBNull.Value)
        {
            ClientReferenceId = dtr.ItemArray[50].ToString();
        }
        if (dtr.ItemArray[51] != DBNull.Value)
        {
            MarketPlaceFeeAmount = Convert.ToDecimal(dtr.ItemArray[51]);
        }
        if (dtr.ItemArray[52] != DBNull.Value)
        {
            MarketPlaceFeeRate = Convert.ToDecimal(dtr.ItemArray[52]);
        }
        if (dtr.ItemArray[53] != DBNull.Value)
        {
            MarketPlaceVatAmount = Convert.ToDecimal(dtr.ItemArray[53]);
        }
        if (dtr.ItemArray[54] != DBNull.Value)
        {
            MarketPlaceVatRate = Convert.ToDecimal(dtr.ItemArray[54]);
        }
        if (dtr.ItemArray[55] != DBNull.Value)
        {
            PayerReferenceId = dtr.ItemArray[55].ToString();
        }
        if (dtr.ItemArray[56] != DBNull.Value)
        {
            MarketPlaceExchangeRate = Convert.ToDecimal(dtr.ItemArray[56]);
        }
        if (dtr.ItemArray[57] != DBNull.Value)
        {
            MarketPlaceFeeAmountUsd = Convert.ToDecimal(dtr.ItemArray[57]);
        }
        if (dtr.ItemArray[58] != DBNull.Value)
        {
            MarketPlaceVatAmountUsd = Convert.ToDecimal(dtr.ItemArray[58]);
        }
        if (dtr.ItemArray[59] != DBNull.Value)
        {
            ForwardedAt = Convert.ToDateTime(dtr.ItemArray[59]);
        }
        if (dtr.ItemArray[60] != DBNull.Value)
        {
            LastPushedAt = Convert.ToDateTime(dtr.ItemArray[60]);
        }
        if (dtr.ItemArray[61] != DBNull.Value)
        {
            TransactionType = Convert.ToInt32(dtr.ItemArray[61]);
        }
    }

    public TransactionalBase(TransactionalBase tb)
    {
        Id = tb.Id;
        LedgerId = tb.LedgerId;
        Source = tb.Source;
        CreatedAt = tb.CreatedAt;
        SourceCountryName = tb.SourceCountryName;
        SourceCountryCode = tb.SourceCountryCode;
        TargetCountryName = tb.TargetCountryName;
        TargetCountryCode = tb.TargetCountryCode;
        Status = tb.Status;
        CollectMethod = tb.CollectMethod;
        Client = tb.Client;
        Obpartner = tb.Obpartner;
        ObpartnerName = tb.ObpartnerName;
        SourceCurrency = tb.SourceCurrency;
        TargetCurrency = tb.TargetCurrency;
        NetAmountSc = tb.NetAmountSc;
        if (tb.Source == "MoneyTransfer")
        {
            if (tb.Client == "REMITEE")
            {
                if (tb.SourceCurrency == "ARS" && tb.TargetCurrency == "ARS")
                {
                    NetAmountUsd = tb.NetAmountSc / tb.ArsexchangeRate;
                }
                if (tb.SourceCurrency == "CLP" && tb.TargetCurrency == "ARS")
                {
                    NetAmountUsd = tb.NetAmountSc / tb.ExchangeRateSc;
                }
                if(!(tb.SourceCurrency == "ARS" && tb.TargetCurrency == "ARS") && !(tb.SourceCurrency == "CLP" && tb.TargetCurrency == "ARS"))
                {
                    NetAmountUsd = tb.NetAmountUsd;
                }
            }
            else
            {
                NetAmountUsd = tb.NetAmountUsd;
            }
        }
        if (tb.Source == "Ledger")
        {
            if (tb.TargetCountryCode == "ARG")
            {
                NetAmountUsd = tb.TargetAmountTc / tb.ArsexchangeRate;
            }
            if (tb.TargetCountryCode == "CHL")
            {
                NetAmountUsd = tb.TargetAmountTc / tb.ClpexchangeRate;
            }
            if (tb.TargetCountryCode != "ARG" && tb.TargetCountryCode != "CHL")
            {
                NetAmountUsd = tb.NetAmountUsd;
            }
        }
        if (tb.Source == "Wallet")
        {
            if (tb.SourceCurrency == "CLP")
            {
                NetAmountUsd = tb.NetAmountSc / tb.ClpexchangeRate;
            }
            if (tb.SourceCurrency == "ARS")
            {
                NetAmountUsd = tb.NetAmountSc / tb.ArsexchangeRate;
            }
            if (tb.SourceCurrency != "CLP" && tb.SourceCurrency != "ARS")
            {
                NetAmountUsd = 0;
            }
        }
        GrossAmountSc = tb.GrossAmountSc;
        ExchangeRateSc = tb.ExchangeRateSc;
        SpreadRate = tb.SpreadRate;
        FeeAmountSc = tb.FeeAmountSc;
        if (tb.Source == "MoneyTransfer")
        {
            if (tb.Client == "REMITEE" && tb.SourceCurrency == "ARS")
            {
                FeeAmountUsd = tb.FeeAmountSc / tb.ArsexchangeRate;
                Vatusd = tb.Vatrate * tb.FeeAmountSc / tb.ArsexchangeRate;
            }
            if (tb.Client == "REMITEE" && tb.SourceCurrency == "CLP")
            {
                FeeAmountUsd = tb.FeeAmountSc / tb.ExchangeRateSc;
                Vatusd = tb.Vatrate * tb.FeeAmountSc / tb.ExchangeRateSc;
            }
            if (tb.Client != "REMITEE" || (tb.SourceCurrency != "CLP" && tb.SourceCurrency != "ARS"))
            {
                FeeAmountUsd = tb.FeeAmountUsd;
                Vatusd = tb.Vatusd;
            }
        }
        if (tb.Source == "Ledger")
        {
            if (tb.TargetCountryCode == "ARG")
            {
                FeeAmountUsd = tb.TargetAmountTc * tb.FeeRate / tb.ArsexchangeRate;
                Vatusd = tb.Vatrate * tb.TargetAmountTc * tb.FeeRate / tb.ArsexchangeRate;
            }
            if (tb.TargetCountryCode == "CHL")
            {
                FeeAmountUsd = tb.TargetAmountTc * tb.FeeRate / tb.ClpexchangeRate;
                Vatusd = tb.Vatrate * tb.TargetAmountTc * tb.FeeRate / tb.ClpexchangeRate;
            }
            if (tb.TargetCountryCode != "ARG" && tb.TargetCountryCode != "CHL")
            {
                FeeAmountUsd = tb.NetAmountUsd * tb.FeeRate;
                Vatusd = tb.Vatrate * tb.NetAmountUsd * tb.FeeRate;
            }
        }
        if (tb.Source == "Wallet")
        {
            if (tb.SourceCurrency == "CLP")
            {
                FeeAmountUsd = tb.NetAmountSc * (decimal)0.11 / tb.ClpexchangeRate;
                Vatusd = 0;
            }
            if (tb.SourceCurrency == "ARS")
            {
                FeeAmountUsd = tb.NetAmountSc * (decimal)0.13 / tb.ArsexchangeRate;
                Vatusd = tb.NetAmountSc * (decimal)0.03 * tb.Vatrate / tb.ArsexchangeRate;
            }
            if (tb.SourceCurrency != "CLP" && tb.SourceCurrency != "ARS")
            {
                FeeAmountUsd = 0;
            }
        }
        FeeRate = tb.FeeRate;
        Vatsc = tb.Vatsc;
        Vatrate = tb.Vatrate;
        TargetAmountTc = tb.TargetAmountTc;
        ExchangeRateTc = tb.ExchangeRateTc;
        MarketExchangeRate = tb.MarketExchangeRate;
        PayerRoute = tb.PayerRoute;
        Payer = tb.Payer;
        ArsexchangeRate = tb.ArsexchangeRate;
        ClpexchangeRate = tb.ClpexchangeRate;
        ArsrexchangeRate = tb.ArsrexchangeRate;
        ClprexchangeRate = tb.ClprexchangeRate;
        TargetAmountTcwithoutWithholding = tb.TargetAmountTcwithoutWithholding;
        WithholdingIncomeAmount = tb.WithholdingIncomeAmount;
        WithholdingVatAmount = tb.WithholdingVatAmount;
        WithholdingIncomeRate = tb.WithholdingIncomeRate;
        WithholdingVatRate = tb.WithholdingVatRate;
        AccountingFxRate = tb.AccountingFxRate;
        AccountingFxRateWithoutSp = tb.AccountingFxRateWithoutSp;
        AccountingNetAmount = tb.AccountingNetAmount;
        SpreadAmountSc = tb.SpreadAmountSc;
        AccountingAgentCommission = tb.AccountingAgentCommission;
        SettledAt = tb.SettledAt;
        CompletedAt = tb.CompletedAt;
        ReversedAt = tb.ReversedAt;
        ClientReferenceId = tb.ClientReferenceId;
        MarketPlaceFeeAmount = tb.MarketPlaceFeeAmount;
        MarketPlaceFeeRate = tb.MarketPlaceFeeRate;
        MarketPlaceVatAmount = tb.MarketPlaceVatAmount;
        MarketPlaceVatRate = tb.MarketPlaceVatRate;
        MarketPlaceVatAmountUsd = tb.MarketPlaceVatAmountUsd;
        MarketPlaceFeeAmountUsd = tb.MarketPlaceFeeAmountUsd;
        Vendor = tb.Vendor;
        PayerReferenceId = tb.PayerReferenceId;
        MarketPlaceExchangeRate = tb.MarketPlaceExchangeRate;
        BillingClientDocumentNumber = tb.BillingClientDocumentNumber;
        BillingClientName = tb.BillingClientName;
        BillingNetTaxed = tb.BillingNetTaxed;
        BillingSiiFolio = tb.BillingSiiFolio;
        BillingTaxAmount = tb.BillingTaxAmount;
        BillingTotalAmount = tb.BillingTotalAmount;
        ArsExchangeRateOf = tb.ArsExchangeRateOf;
        ForwardedAt = tb.ForwardedAt;
        LastPushedAt = tb.LastPushedAt;
        CreatedAtFixed = tb.CreatedAtFixed;
        SettledAtFixed = tb.SettledAtFixed;
        CompletedAtFixed = tb.CompletedAtFixed;
        ReversedAtFixed = tb.ReversedAtFixed;
        ForwardedAtFixed = tb.ForwardedAtFixed;
        LastPushedAtFixed = tb.LastPushedAtFixed;
        StatusFixed = tb.StatusFixed;
        if (tb.Source == "MoneyTransfer")
        {
            if (tb.Client == "REMITEE")
            {
                if (tb.SourceCurrency == "ARS")
                {
                    if (tb.TargetCurrency == "ARS")
                    {
                        SpreadAmountUsd = tb.NetAmountSc / tb.ArsexchangeRate - tb.TargetAmountTc / tb.ArsexchangeRate;
                    }
                    else
                    {
                        SpreadAmountUsd = tb.NetAmountSc / tb.ArsexchangeRate - tb.NetAmountUsd;
                    }
                }
                else
                {
                    if (tb.TargetCurrency == "ARS")
                    {
                        SpreadAmountUsd = tb.NetAmountSc / tb.ExchangeRateSc - tb.TargetAmountTc / tb.ArsexchangeRate;
                    }
                    else
                    {
                        SpreadAmountUsd = tb.NetAmountSc / tb.ExchangeRateSc - tb.NetAmountUsd;
                    }
                }
            }
            else
            {
                if (tb.TargetCurrency == "ARS")
                {
                    SpreadAmountUsd = tb.NetAmountUsd - tb.TargetAmountTc / tb.ArsexchangeRate ;
                    SpreadAmountSc = tb.NetAmountUsd - tb.TargetAmountTc / tb.ArsexchangeRate;
                }
                else if (tb.SpreadRate == null)
                {
                    SpreadAmountUsd = 0;
                }
                else if (tb.TargetCurrency == "VEF")
                {
                    
                    SpreadAmountUsd = tb.NetAmountUsd * tb.SpreadRate;
                    
                }
                else
                {
                    SpreadAmountUsd = tb.NetAmountUsd - tb.TargetAmountTc / tb.MarketExchangeRate;
                }
            }
        }

    }
}

public partial class TransactionalBaseDTO
{
    public string Id { get; set; } = null!;

    public int? LedgerId { get; set; }

    public string? Source { get; set; }

    public DateTime CreatedAt { get; set; }

    public string? SourceCountryName { get; set; }

    public string? SourceCountryCode { get; set; }

    public string? TargetCountryName { get; set; }

    public string? TargetCountryCode { get; set; }

    public string? Status { get; set; }

    public string? CollectMethod { get; set; }

    public string? Client { get; set; }

    public string? Obpartner { get; set; }

    public string? ObpartnerName { get; set; }

    public string? SourceCurrency { get; set; }

    public string? TargetCurrency { get; set; }

    public decimal? NetAmountSc { get; set; }

    public decimal? NetAmountUsd { get; set; }

    public decimal? GrossAmountSc { get; set; }

    public decimal? ExchangeRateSc { get; set; }

    public decimal? SpreadRate { get; set; }

    public decimal? FeeAmountSc { get; set; }

    public decimal? FeeAmountUsd { get; set; }

    public decimal? FeeRate { get; set; }

    public decimal? Vatsc { get; set; }

    public decimal? Vatusd { get; set; }

    public decimal? Vatrate { get; set; }

    public decimal? TargetAmountTc { get; set; }

    public decimal? TargetAmountTcwithoutWithholding { get; set; }

    public decimal? WithholdingIncomeAmount { get; set; }

    public decimal? WithholdingVatAmount { get; set; }

    public decimal? WithholdingIncomeRate { get; set; }

    public decimal? WithholdingVatRate { get; set; }

    public decimal? ExchangeRateTc { get; set; }

    public decimal? MarketExchangeRate { get; set; }

    public string? PayerRoute { get; set; }

    public string? Payer { get; set; }

    public decimal? AccountingFxRate { get; set; }

    public decimal? AccountingFxRateWithoutSp { get; set; }

    public decimal? AccountingNetAmount { get; set; }

    public decimal? SpreadAmountSc { get; set; }

    public string? ClientReferenceId { get; set; }

    public decimal? MarketPlaceFeeAmount { get; set; }

    public decimal? MarketPlaceFeeRate { get; set; }

    public decimal? MarketPlaceVatAmount { get; set; }

    public decimal? MarketPlaceVatRate { get; set; }

    public string? Vendor { get; set; }

    public DateTime? SettledAt { get; set; }

    public DateTime? CompletedAt { get; set; }

    public DateTime? ReversedAt { get; set; }

    public string? PayerReferenceId { get; set; }

    public decimal? MarketPlaceExchangeRate { get; set; }

    public decimal? MarketPlaceFeeAmountUsd { get; set; }

    public decimal? MarketPlaceVatAmountUsd { get; set; }

    public int? TransactionType { get; set; }

}
