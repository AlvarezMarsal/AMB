namespace AmbHelper;

/// <summary>
/// A POCO class to hold settings for the ImportLocations application.
/// </summary>
public partial class Settings
{
    public string ConnectionString { get; set; } = "";
    public long CreatorId { get; set; } = 1;
    public string CreationSession { get; set; } = Guid.NewGuid().ToString();
    public List<Preset> Presets { get; set; } = [];
    public List<Import> Imports { get; set; } = [];
    public string NameCollation { get; set; } = "SQL_Latin1_General_CP1_CI_AS";
    public string AliasCollation { get; set; } = "SQL_Latin1_General_CP1_CI_AS";
}
