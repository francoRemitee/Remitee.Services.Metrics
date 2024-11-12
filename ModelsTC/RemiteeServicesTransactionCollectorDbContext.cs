using System;
using System.Collections.Generic;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;

namespace Remitee.Services.Metrics.ModelsTC;

public partial class RemiteeServicesTransactionCollectorDbContext : DbContext
{
    static IConfigurationRoot _config;
    public RemiteeServicesTransactionCollectorDbContext(IConfigurationRoot config)
    {
        _config = config;
        Database.SetCommandTimeout(5000);
    }

    public RemiteeServicesTransactionCollectorDbContext(DbContextOptions<RemiteeServicesTransactionCollectorDbContext> options)
        : base(options)
    {
    }

    public virtual DbSet<Country> Countries { get; set; }

    public virtual DbSet<Receiver> Receivers { get; set; }

    public virtual DbSet<Sender> Senders { get; set; }

    public virtual DbSet<Transaction> Transactions { get; set; }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.UseSqlServer(_config.GetConnectionString("TransactionCollectorConnString"), builder =>
        {
            builder.EnableRetryOnFailure(5, TimeSpan.FromSeconds(10), null);
        });
        base.OnConfiguring(optionsBuilder);
    }
    
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Country>(entity =>
        {
            entity.ToTable("Countries", "tc");

            entity.Property(e => e.Id).HasMaxLength(3);
            entity.Property(e => e.Isothree).HasColumnName("ISOThree");
            entity.Property(e => e.Isotwo).HasColumnName("ISOTwo");
        });

        modelBuilder.Entity<Receiver>(entity =>
        {
            entity.ToTable("Receivers", "tc");

            entity.HasIndex(e => e.AccountId, "IX_Receivers_AccountId");
        });

        modelBuilder.Entity<Sender>(entity =>
        {
            entity.ToTable("Senders", "tc");

            entity.HasIndex(e => e.AccountId, "IX_Senders_AccountId");
        });

        modelBuilder.Entity<Transaction>(entity =>
        {
            entity.ToTable("Transactions", "tc");

            entity.Property(e => e.ReceiverAmount).HasColumnType("decimal(18, 2)");
            entity.Property(e => e.SenderAmount).HasColumnType("decimal(18, 2)");
            entity.Property(e => e.TrxAmount).HasColumnType("decimal(18, 2)");
        });

        OnModelCreatingPartial(modelBuilder);
    }

    partial void OnModelCreatingPartial(ModelBuilder modelBuilder);
}
