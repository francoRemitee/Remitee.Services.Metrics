using System;
using System.Collections.Generic;
using Microsoft.EntityFrameworkCore;

namespace Remitee.Services.Metrics.Models;

public partial class RemiteeServicesMetricsContext : DbContext
{
    public RemiteeServicesMetricsContext()
    {
    }

    public RemiteeServicesMetricsContext(DbContextOptions<RemiteeServicesMetricsContext> options)
        : base(options)
    {
    }

    public virtual DbSet<BasicChurn> BasicChurns { get; set; }

    public virtual DbSet<ComplexChurn> ComplexChurns { get; set; }

    public virtual DbSet<Corredore> Corredores { get; set; }

    public virtual DbSet<ExchangeRate> ExchangeRates { get; set; }

    public virtual DbSet<Inbound> Inbounds { get; set; }

    public virtual DbSet<NewSender> NewSenders { get; set; }

    public virtual DbSet<Receiver> Receivers { get; set; }

    public virtual DbSet<Registrating> Registratings { get; set; }

    public virtual DbSet<Sender> Senders { get; set; }

    public virtual DbSet<SendersBreakdown> SendersBreakdowns { get; set; }

    public virtual DbSet<Tccountry> Tccountries { get; set; }

    public virtual DbSet<Tcreceiver> Tcreceivers { get; set; }

    public virtual DbSet<Tcsender> Tcsenders { get; set; }

    public virtual DbSet<Tctransaction> Tctransactions { get; set; }

    public virtual DbSet<TransactionalBase> TransactionalBases { get; set; }

    public virtual DbSet<PartnersOperation> PartnersOperations { get; set; }

    public virtual DbSet<FlatTransaction> FlatTransactions { get; set; }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.UseSqlServer("Server=tcp:remiteesql-dev.database.windows.net,1433;Database=Remitee.Services.Dev.Metrics;User Id=Franco.Zeppilli;Password=nQSLT6kg;MultipleActiveResultSets=True;", builder =>
        {
            builder.EnableRetryOnFailure(5, TimeSpan.FromSeconds(10), null);
        });
        base.OnConfiguring(optionsBuilder);
    }


    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<BasicChurn>(entity =>
        {
            entity.ToTable("BasicChurn");

            entity.Property(e => e.Id).ValueGeneratedNever();
            entity.Property(e => e.CountryCode).HasMaxLength(3);
            entity.Property(e => e.CountryName).HasMaxLength(50);
        });

        modelBuilder.Entity<ComplexChurn>(entity =>
        {
            entity.ToTable("ComplexChurn");

            entity.Property(e => e.Id).ValueGeneratedNever();
            entity.Property(e => e.CountMt).HasColumnName("Count_MT");
            entity.Property(e => e.CountTopup).HasColumnName("Count_TOPUP");
            entity.Property(e => e.Gtv)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("GTV");
            entity.Property(e => e.GtvAvg)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("GTV_AVG");
            entity.Property(e => e.GtvMt)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("GTV_MT");
            entity.Property(e => e.GtvTopup)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("GTV_TOPUP");
            entity.Property(e => e.Partner).HasMaxLength(50);
            entity.Property(e => e.SourceCountryCode).HasMaxLength(3);
            entity.Property(e => e.SourceCountryName).HasMaxLength(50);
            entity.Property(e => e.TargetCountryCode).HasMaxLength(3);
            entity.Property(e => e.TargetCountryName).HasMaxLength(50);
            entity.Property(e => e.Type).HasMaxLength(10);
        });

        modelBuilder.Entity<Corredore>(entity =>
        {
            entity.Property(e => e.Id).ValueGeneratedNever();
            entity.Property(e => e.CountInbound).HasColumnName("Count_Inbound");
            entity.Property(e => e.CountMt).HasColumnName("Count_MT");
            entity.Property(e => e.CountRemitee).HasColumnName("Count_Remitee");
            entity.Property(e => e.CountTopup).HasColumnName("Count_TOPUP");
            entity.Property(e => e.FeeInbound)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("Fee_Inbound");
            entity.Property(e => e.FeeRemitee)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("Fee_Remitee");
            entity.Property(e => e.FeeTotal)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("Fee_Total");
            entity.Property(e => e.GtvInbound)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("GTV_Inbound");
            entity.Property(e => e.GtvMt)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("GTV_MT");
            entity.Property(e => e.GtvRemitee)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("GTV_Remitee");
            entity.Property(e => e.GtvTopup)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("GTV_TOPUP");
            entity.Property(e => e.GtvTotal)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("GTV_Total");
            entity.Property(e => e.NtvInbound)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("NTV_Inbound");
            entity.Property(e => e.NtvMt)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("NTV_MT");
            entity.Property(e => e.NtvRemitee)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("NTV_Remitee");
            entity.Property(e => e.NtvTopup)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("NTV_TOPUP");
            entity.Property(e => e.NtvTotal)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("NTV_Total");
            entity.Property(e => e.SourceCountryCode).HasMaxLength(3);
            entity.Property(e => e.SourceCountryName).HasMaxLength(50);
            entity.Property(e => e.SpreadInbound)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("Spread_Inbound");
            entity.Property(e => e.SpreadRemitee)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("Spread_Remitee");
            entity.Property(e => e.SpreadTotal)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("Spread_Total");
            entity.Property(e => e.TargetCountryCode).HasMaxLength(3);
            entity.Property(e => e.TargetCountryName).HasMaxLength(50);
        });

        modelBuilder.Entity<ExchangeRate>(entity =>
        {
            entity.Property(e => e.Id).ValueGeneratedNever();
            entity.Property(e => e.CountryCode).HasMaxLength(3);
            entity.Property(e => e.CountryName).HasMaxLength(50);
            entity.Property(e => e.Date).HasColumnType("date");
            entity.Property(e => e.ExchangeRate1)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("ExchangeRate");
            entity.Property(e => e.OriginCurrency).HasMaxLength(3);
            entity.Property(e => e.TargetCurrency).HasMaxLength(3);
        });

        modelBuilder.Entity<Inbound>(entity =>
        {
            entity.ToTable("Inbound");

            entity.Property(e => e.Id).ValueGeneratedNever();
            entity.Property(e => e.CountMt).HasColumnName("Count_MT");
            entity.Property(e => e.CountTopup).HasColumnName("Count_TOPUP");
            entity.Property(e => e.Fee).HasColumnType("decimal(20, 8)");
            entity.Property(e => e.GtvAvg)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("GTV_AVG");
            entity.Property(e => e.GtvMt)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("GTV_MT");
            entity.Property(e => e.GtvTopup)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("GTV_TOPUP");
            entity.Property(e => e.GtvTotal)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("GTV_Total");
            entity.Property(e => e.NtvMt)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("NTV_MT");
            entity.Property(e => e.NtvTopup)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("NTV_TOPUP");
            entity.Property(e => e.NtvTotal)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("NTV_Total");
            entity.Property(e => e.Partner).HasMaxLength(50);
            entity.Property(e => e.SourceCountryCode).HasMaxLength(3);
            entity.Property(e => e.SourceCountryName).HasMaxLength(50);
            entity.Property(e => e.Spread).HasColumnType("decimal(20, 8)");
            entity.Property(e => e.TargetCountryCode).HasMaxLength(3);
            entity.Property(e => e.TargetCountryName).HasMaxLength(50);
            entity.Property(e => e.Vat)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("VAT");
        });

        modelBuilder.Entity<NewSender>(entity =>
        {
            entity.Property(e => e.Id).ValueGeneratedNever();
            entity.Property(e => e.CountryCode).HasMaxLength(3);
            entity.Property(e => e.CountryName).HasMaxLength(30);
            entity.Property(e => e.Type).HasMaxLength(3);
        });

        modelBuilder.Entity<Receiver>(entity =>
        {
            entity.Property(e => e.Id).ValueGeneratedNever();
            entity.Property(e => e.CountryCode).HasMaxLength(3);
            entity.Property(e => e.CountryName).HasMaxLength(50);
            entity.Property(e => e.Description).HasMaxLength(100);
            entity.Property(e => e.Type).HasMaxLength(3);
        });

        modelBuilder.Entity<Registrating>(entity =>
        {
            entity.ToTable("Registrating");

            entity.Property(e => e.Id).ValueGeneratedNever();
            entity.Property(e => e.CountryCode).HasMaxLength(3);
            entity.Property(e => e.CountryName)
                .HasMaxLength(10)
                .IsFixedLength();
        });

        modelBuilder.Entity<Sender>(entity =>
        {
            entity.Property(e => e.Id).ValueGeneratedNever();
            entity.Property(e => e.CountryCode).HasMaxLength(3);
            entity.Property(e => e.CountryName).HasMaxLength(30);
            entity.Property(e => e.Type).HasMaxLength(3);
        });

        modelBuilder.Entity<SendersBreakdown>(entity =>
        {
            entity.ToTable("SendersBreakdown");

            entity.Property(e => e.Id).ValueGeneratedNever();
            entity.Property(e => e.Bothcount).HasColumnName("BOTHCount");
            entity.Property(e => e.CountryCode).HasMaxLength(3);
            entity.Property(e => e.CountryName).HasMaxLength(50);
            entity.Property(e => e.OnlyMtcount).HasColumnName("OnlyMTCount");
            entity.Property(e => e.OnlyTopupcount).HasColumnName("OnlyTOPUPCount");
            entity.Property(e => e.Type).HasMaxLength(3);
        });

        modelBuilder.Entity<Tccountry>(entity =>
        {
            entity.ToTable("TCCountries", "tc");

            entity.Property(e => e.Id).HasMaxLength(3);
            entity.Property(e => e.Isothree).HasColumnName("ISOThree");
            entity.Property(e => e.Isotwo).HasColumnName("ISOTwo");
        });

        modelBuilder.Entity<Tcreceiver>(entity =>
        {
            entity.HasKey(e => e.Id).HasName("PK__TCReceiv__3214EC070F7CF63E");

            entity.ToTable("TCReceivers", "tc");

            entity.Property(e => e.DateCreated).HasPrecision(0);
            entity.Property(e => e.DateModified).HasPrecision(0);
            entity.Property(e => e.DateOfBirth).HasPrecision(0);
            entity.Property(e => e.FileDateCreated).HasPrecision(0);
        });

        modelBuilder.Entity<Tcsender>(entity =>
        {
            entity.HasKey(e => e.Id).HasName("PK__TCTransa__3214EC075592AB3D");

            entity.ToTable("TCSenders", "tc");

            entity.Property(e => e.DateCreated).HasPrecision(0);
            entity.Property(e => e.DateModified).HasPrecision(0);
            entity.Property(e => e.DateOfBirth).HasPrecision(0);
            entity.Property(e => e.FileDateCreated).HasPrecision(0);
        });

        modelBuilder.Entity<Tctransaction>(entity =>
        {
            entity.HasKey(e => e.Id).HasName("PK__TCTransa__3214EC075592AB3D");

            entity.ToTable("TCTransactions", "tc");

            entity.Property(e => e.Id).ValueGeneratedNever();
            entity.Property(e => e.DateCompleted).HasPrecision(0);
            entity.Property(e => e.DateCreated).HasPrecision(0);
            entity.Property(e => e.FileDateCreated).HasPrecision(0);
            entity.Property(e => e.ReceiverAmount).HasColumnType("decimal(18, 2)");
            entity.Property(e => e.ReceiverId).HasMaxLength(450);
            entity.Property(e => e.SenderAmount).HasColumnType("decimal(18, 2)");
            entity.Property(e => e.SenderId).HasMaxLength(450);
            entity.Property(e => e.TrxAmount).HasColumnType("decimal(18, 2)");
            entity.Property(e => e.TrxDate).HasPrecision(0);
        });

        modelBuilder.Entity<TransactionalBase>(entity =>
        {
            entity.HasKey(e => e.Id).HasName("PK__Transact__3214EC07850BA975");

            entity.ToTable("TransactionalBase");

            entity.Property(e => e.Id).HasMaxLength(50);
            entity.Property(e => e.AccountingAgentCommission).HasColumnType("decimal(20, 8)");
            entity.Property(e => e.AccountingFxRate).HasColumnType("decimal(20, 8)");
            entity.Property(e => e.AccountingFxRateWithoutSp).HasColumnType("decimal(20, 8)");
            entity.Property(e => e.AccountingNetAmount).HasColumnType("decimal(20, 8)");
            entity.Property(e => e.ArsexchangeRate)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("ARSExchangeRate");
            entity.Property(e => e.ArsrexchangeRate)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("ARSRExchangeRate");
            entity.Property(e => e.Client).HasMaxLength(50);
            entity.Property(e => e.ClientReferenceId).HasMaxLength(100);
            entity.Property(e => e.ClpexchangeRate)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("CLPExchangeRate");
            entity.Property(e => e.ClprexchangeRate)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("CLPRExchangeRate");
            entity.Property(e => e.CollectMethod).HasMaxLength(20);
            entity.Property(e => e.CompletedAt).HasPrecision(0);
            entity.Property(e => e.CreatedAt).HasPrecision(0);
            entity.Property(e => e.ExchangeRateSc)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("ExchangeRateSC");
            entity.Property(e => e.ExchangeRateTc)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("ExchangeRateTC");
            entity.Property(e => e.FeeAmountSc)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("FeeAmountSC");
            entity.Property(e => e.FeeAmountUsd)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("FeeAmountUSD");
            entity.Property(e => e.FeeRate).HasColumnType("decimal(20, 8)");
            entity.Property(e => e.GrossAmountSc)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("GrossAmountSC");
            entity.Property(e => e.MarketExchangeRate).HasColumnType("decimal(20, 8)");
            entity.Property(e => e.MarketPlaceExchangeRate).HasColumnType("decimal(20, 8)");
            entity.Property(e => e.MarketPlaceFeeAmount).HasColumnType("decimal(20, 8)");
            entity.Property(e => e.MarketPlaceFeeAmountUsd)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("MarketPlaceFeeAmountUSD");
            entity.Property(e => e.MarketPlaceFeeRate).HasColumnType("decimal(20, 8)");
            entity.Property(e => e.MarketPlaceVatAmount)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("MarketPlaceVATAmount");
            entity.Property(e => e.MarketPlaceVatAmountUsd)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("MarketPlaceVATAmountUSD");
            entity.Property(e => e.MarketPlaceVatRate)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("MarketPlaceVATRate");
            entity.Property(e => e.NetAmountSc)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("NetAmountSC");
            entity.Property(e => e.NetAmountUsd)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("NetAmountUSD");
            entity.Property(e => e.Obpartner)
                .HasMaxLength(100)
                .HasColumnName("OBPartner");
            entity.Property(e => e.ObpartnerName)
                .HasMaxLength(100)
                .HasColumnName("OBPartnerName");
            entity.Property(e => e.Payer).HasMaxLength(50);
            entity.Property(e => e.PayerExchangeRate).HasColumnType("decimal(20, 8)");
            entity.Property(e => e.PayerExchangeRateExpected).HasColumnType("decimal(20, 8)");
            entity.Property(e => e.PayerFee).HasColumnType("decimal(20, 8)");
            entity.Property(e => e.PayerFeeExpected).HasColumnType("decimal(20, 8)");
            entity.Property(e => e.PayerReferenceId).HasMaxLength(100);
            entity.Property(e => e.PayerRoute).HasMaxLength(80);
            entity.Property(e => e.RemiteeCalculatedFee).HasColumnType("decimal(20, 8)");
            entity.Property(e => e.ReversedAt).HasPrecision(0);
            entity.Property(e => e.SettledAt).HasPrecision(0);
            entity.Property(e => e.Source).HasMaxLength(20);
            entity.Property(e => e.SourceCountryCode).HasMaxLength(3);
            entity.Property(e => e.SourceCountryName).HasMaxLength(50);
            entity.Property(e => e.SourceCurrency).HasMaxLength(3);
            entity.Property(e => e.SpreadAmountSc)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("SpreadAmountSC");
            entity.Property(e => e.SpreadAmountUsd)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("SpreadAmountUSD");
            entity.Property(e => e.SpreadRate).HasColumnType("decimal(20, 8)");
            entity.Property(e => e.Status).HasMaxLength(20);
            entity.Property(e => e.TargetAmountTc)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("TargetAmountTC");
            entity.Property(e => e.TargetAmountTcwithoutWithholding)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("TargetAmountTCwithoutWithholding");
            entity.Property(e => e.TargetCountryCode).HasMaxLength(3);
            entity.Property(e => e.TargetCountryName).HasMaxLength(50);
            entity.Property(e => e.TargetCurrency).HasMaxLength(3);
            entity.Property(e => e.Vatrate)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("VATRate");
            entity.Property(e => e.Vatsc)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("VATSC");
            entity.Property(e => e.Vatusd)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("VATUSD");
            entity.Property(e => e.Vendor).HasMaxLength(50);
            entity.Property(e => e.WithholdingIncomeAmount).HasColumnType("decimal(20, 8)");
            entity.Property(e => e.WithholdingIncomeRate).HasColumnType("decimal(20, 8)");
            entity.Property(e => e.WithholdingVatAmount)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("WithholdingVATAmount");
            entity.Property(e => e.WithholdingVatRate)
                .HasColumnType("decimal(20, 8)")
                .HasColumnName("WithholdingVATRate");
        });

        modelBuilder.Entity<PartnersOperation>(entity =>
        {
            entity.ToTable("PartnersOperations");
            entity.Property(e => e.Id).ValueGeneratedNever();
            entity.Property(e => e.Type).HasMaxLength(6);
            entity.Property(e => e.Description).HasMaxLength(100);
            entity.Property(e => e.SourceCurrency).HasMaxLength(3);
            entity.Property(e => e.TargetCurrency).HasMaxLength(3);
            entity.Property(e => e.Partner).HasMaxLength(50);
            entity.Property(e => e.Amount).HasColumnType("decimal(20, 8)");
            entity.Property(e => e.ExchangeRate).HasColumnType("decimal(20, 8)");
        });

        modelBuilder.Entity<FlatTransaction>(entity =>
        {
            entity.HasKey(e => e.Id).HasName("PK__FlatTran__00DD96F71765B8B4");

            entity.Property(e => e.Id).ValueGeneratedNever();
            entity.Property(e => e.ReceiverAddressLine).HasMaxLength(200);
            entity.Property(e => e.ReceiverBankAccountBic)
                .HasMaxLength(11)
                .HasColumnName("ReceiverBankAccountBIC");
            entity.Property(e => e.ReceiverBankAccountNumber).HasMaxLength(100);
            entity.Property(e => e.ReceiverBankAccountType).HasMaxLength(7);
            entity.Property(e => e.ReceiverCompleteName).HasMaxLength(200);
            entity.Property(e => e.ReceiverCountry).HasMaxLength(3);
            entity.Property(e => e.ReceiverCountryOfBirth).HasMaxLength(3);
            entity.Property(e => e.ReceiverCountrySubDivision).HasMaxLength(100);
            entity.Property(e => e.ReceiverDateOfBirth).HasPrecision(0);
            entity.Property(e => e.ReceiverDocumentIssuer).HasMaxLength(3);
            entity.Property(e => e.ReceiverDocumentNumber).HasMaxLength(50);
            entity.Property(e => e.ReceiverDocumentType).HasMaxLength(7);
            entity.Property(e => e.ReceiverEmail).HasMaxLength(200);
            entity.Property(e => e.ReceiverFirstName).HasMaxLength(100);
            entity.Property(e => e.ReceiverLastName).HasMaxLength(100);
            entity.Property(e => e.ReceiverNationality).HasMaxLength(3);
            entity.Property(e => e.ReceiverPhoneNumber).HasMaxLength(50);
            entity.Property(e => e.ReceiverPostalCode).HasMaxLength(50);
            entity.Property(e => e.ReceiverReceivingCountry).HasMaxLength(3);
            entity.Property(e => e.ReceiverTaxId).HasMaxLength(100);
            entity.Property(e => e.ReceiverTown).HasMaxLength(100);
            entity.Property(e => e.ReceiverType).HasMaxLength(15);
            entity.Property(e => e.SenderAddressLine).HasMaxLength(200);
            entity.Property(e => e.SenderBankAccountBic)
                .HasMaxLength(11)
                .HasColumnName("SenderBankAccountBIC");
            entity.Property(e => e.SenderBankAccountNumber).HasMaxLength(100);
            entity.Property(e => e.SenderBankAccountType).HasMaxLength(7);
            entity.Property(e => e.SenderCompleteName).HasMaxLength(200);
            entity.Property(e => e.SenderCountry).HasMaxLength(3);
            entity.Property(e => e.SenderCountryOfBirth).HasMaxLength(3);
            entity.Property(e => e.SenderCountrySubdivision).HasMaxLength(100);
            entity.Property(e => e.SenderDateOfBirth).HasPrecision(0);
            entity.Property(e => e.SenderDocumentIssuer).HasMaxLength(3);
            entity.Property(e => e.SenderDocumentNumber).HasMaxLength(50);
            entity.Property(e => e.SenderDocumentType).HasMaxLength(7);
            entity.Property(e => e.SenderEmail).HasMaxLength(200);
            entity.Property(e => e.SenderFirstName).HasMaxLength(100);
            entity.Property(e => e.SenderLastName).HasMaxLength(100);
            entity.Property(e => e.SenderNationality).HasMaxLength(3);
            entity.Property(e => e.SenderPhoneNumber).HasMaxLength(50);
            entity.Property(e => e.SenderPostalCode).HasMaxLength(50);
            entity.Property(e => e.SenderSendingCountry).HasMaxLength(3);
            entity.Property(e => e.SenderTaxId).HasMaxLength(100);
            entity.Property(e => e.SenderTown).HasMaxLength(100);
            entity.Property(e => e.SenderType).HasMaxLength(15);
            

            entity.HasOne(d => d.TransactionCollectorTransaction).WithMany(p => p.FlatTransactions)
                .HasForeignKey(d => d.TransactionCollectorTransactionId)
                .HasConstraintName("FK__FlatTrans__Trans__17F790F9");

            entity.HasOne(d => d.TransactionalBaseTransaction).WithMany(p => p.FlatTransactions)
                .HasForeignKey(d => d.Id)
                .HasConstraintName("FK__FlatTransact__Id__17036CC0");
        });

        OnModelCreatingPartial(modelBuilder);
    }

    partial void OnModelCreatingPartial(ModelBuilder modelBuilder);
}
