namespace AmbHelper;

/// <summary>
/// A POCO class to hold settings for the ImportLocations application.
/// </summary>
public partial class Settings
{
    public class Preset
    {
        public string Name { get; set; } = "";
        public long Oid { get; set; }
        public long? Pid { get; set; }
    }
}
