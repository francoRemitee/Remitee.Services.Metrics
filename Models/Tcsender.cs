using System;
using System.Collections.Generic;

namespace Remitee.Services.Metrics.Models;

public partial class Tcsender
{
    public int Id { get; set; }

    public string AccountId { get; set; } = null!;

    public string? UniquePersonId { get; set; }

    public string? Name { get; set; }

    public bool IsIdentityVerified { get; set; }

    public string? Address { get; set; }

    public string? City { get; set; }

    public string? State { get; set; }

    public string? PostalCode { get; set; }

    public string? Country { get; set; }

    public string? Email { get; set; }

    public DateTime? DateOfBirth { get; set; }

    public DateTime? DateModified { get; set; }

    public DateTime DateCreated { get; set; }

    public bool IsDeleted { get; set; }

    public bool IsBlackListed { get; set; }

    public bool IsBlocked { get; set; }

    public string? RiskLevel { get; set; }

    public string? NationalIdType { get; set; }

    public string? NationalIdCountryOfIssue { get; set; }

    public string? Nationality { get; set; }

    public string? CountryOfBirth { get; set; }

    public string? Occupation { get; set; }

    public bool IsPep { get; set; }

    public bool IsSanctioned { get; set; }

    public string? BusinessSegment { get; set; }

    public DateTime? FileDateCreated { get; set; }

    public string? FileNameSaved { get; set; }
}
