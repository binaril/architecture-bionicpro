namespace ReportsApi.Models;

public class CustomerReport
{
    public int CustomerId { get; set; }
    public string? CustomerName { get; set; }
    public string? Email { get; set; }
    public string? Phone { get; set; }
    public string? Company { get; set; }
    public DateTime? CrmCreatedAt { get; set; }

    public int DeviceCount { get; set; }
    public int ActiveDevices { get; set; }
    public string? DeviceTypes { get; set; }

    public long TotalEvents { get; set; }
    public decimal? AvgEventValue { get; set; }
    public decimal? MaxEventValue { get; set; }
    public decimal? MinEventValue { get; set; }
    public DateTime? FirstEventAt { get; set; }
    public DateTime? LastEventAt { get; set; }

    public long Events30d { get; set; }
    public decimal? AvgValue30d { get; set; }

    public Dictionary<string, object>? EventsByType { get; set; }

    public DateTime MartUpdatedAt { get; set; }
}
