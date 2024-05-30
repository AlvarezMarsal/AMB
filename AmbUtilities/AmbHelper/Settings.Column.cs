namespace AmbHelper;

/// <summary>
/// A POCO class to hold settings for the ImportLocations application.
/// </summary>
public partial class Settings
{
    // Each column contains a string that is the name or alias of some GeographicalLocation.
    // The relationships among the columns are defined by the ColumnDefinitions.
    public class Column
    {
        public string Tag { get; set; } = "";               // A useful tag like 'Country' or 'City'
        public string Name { get; set; } = "";              // Which column in the spreadsheet 'A'
        public List<string> ParentOf { get; set; } = [];    // The value in the column can identify a parent of something in another column
        public string? AliasOf { get; set; } = null;        // The value in the column can be an alias of the entity identified by another column
        public bool MustExist { get; set; } = false;
        public bool Optional { get; set; } = false;
        public bool IsSystemOwned { get; set; } = false;
        public List<string> ExcludedValues { get; set; } = [];
    }
}
