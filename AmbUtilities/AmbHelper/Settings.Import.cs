namespace AmbHelper;

/// <summary>
/// A POCO class to hold settings for the ImportLocations application.
/// </summary>
public partial class Settings
{
    public class Import
    {
        public string FilePath { get; set; } = "";
        public string Sheet { get; set; } = "";
        public int FirstRow { get; set; } = -1;
        public int LastRow { get; set; } = -1;
        public List<Column> Columns { get; set; } = [];    // The entity's parents, grandparents, etc. in ascending order of size (world is last)
    }
}
